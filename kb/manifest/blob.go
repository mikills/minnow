package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

type BlobStore interface {
	Head(ctx context.Context, key string) (*blobstore.ObjectInfo, error)
	DownloadBytes(ctx context.Context, key string) ([]byte, error)
	UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*blobstore.ObjectInfo, error)
	Delete(ctx context.Context, key string) error
}

type BlobStoreManifest struct {
	Store BlobStore
}

func (s *BlobStoreManifest) manifestKey(kbID string) string { return ShardManifestKey(kbID) }

func (s *BlobStoreManifest) Get(ctx context.Context, kbID string) (*Document, error) {
	key := s.manifestKey(kbID)
	info, data, err := s.downloadStableManifestBytes(ctx, key)
	if err != nil {
		return nil, err
	}
	manifest, err := readManifestBytes(data)
	if err != nil {
		return nil, err
	}
	return &Document{Manifest: manifest, Version: info.Version}, nil
}

func (s *BlobStoreManifest) downloadStableManifestBytes(
	ctx context.Context,
	key string,
) (*blobstore.ObjectInfo, []byte, error) {
	const maxAttempts = 1 << 2
	for attempt := range maxAttempts {
		info, data, err := s.downloadManifestBytesAttempt(ctx, key)
		if err == nil {
			return info, data, nil
		}
		if !errors.Is(err, blobstore.ErrVersionMismatch) || attempt == maxAttempts-1 {
			return nil, nil, err
		}
		if err := sleepWithContext(ctx, manifestReadBackoff(attempt)); err != nil {
			return nil, nil, err
		}
	}
	return nil, nil, fmt.Errorf("manifest changed during read: %w", blobstore.ErrVersionMismatch)
}

func (s *BlobStoreManifest) downloadManifestBytesAttempt(
	ctx context.Context,
	key string,
) (*blobstore.ObjectInfo, []byte, error) {
	info, err := s.headManifest(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	data, err := s.Store.DownloadBytes(ctx, key)
	if err != nil {
		return nil, nil, manifestStoreReadError(err)
	}
	latest, err := s.headManifest(ctx, key)
	if err != nil {
		return nil, nil, err
	}
	if latest.Version != info.Version {
		return nil, nil, fmt.Errorf("manifest changed during read: %w", blobstore.ErrVersionMismatch)
	}
	return info, data, nil
}

func (s *BlobStoreManifest) headManifest(ctx context.Context, key string) (*blobstore.ObjectInfo, error) {
	info, err := s.Store.Head(ctx, key)
	if err != nil {
		return nil, manifestStoreReadError(err)
	}
	return info, nil
}

func manifestStoreReadError(err error) error {
	if errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	return err
}

func manifestReadBackoff(attempt int) time.Duration {
	backoff := time.Duration(5<<uint(attempt+1)) * time.Millisecond
	jitter := time.Duration(rand.Int63n(int64(5 * time.Millisecond)))
	return backoff + jitter
}

func readManifestBytes(data []byte) (ShardManifest, error) {
	var manifest ShardManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return ShardManifest{}, err
	}
	applyManifestReadDefaults(&manifest)
	return manifest, nil
}

func applyManifestReadDefaults(manifest *ShardManifest) {
	if manifest.FormatKind == "" {
		manifest.FormatKind = "duckdb_sharded"
	}
	if manifest.FormatVersion <= 0 {
		manifest.FormatVersion = 1
	}
}

func (s *BlobStoreManifest) HeadVersion(ctx context.Context, kbID string) (string, error) {
	info, err := s.Store.Head(ctx, s.manifestKey(kbID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	return info.Version, nil
}

func (s *BlobStoreManifest) UpsertIfMatch(
	ctx context.Context,
	kbID string,
	manifest ShardManifest,
	expectedVersion string,
) (string, error) {
	tmpDir, err := os.MkdirTemp("", "minnow-manifest-upsert-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(tmpDir)
	manifestPath := filepath.Join(tmpDir, "manifest.json")
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		return "", err
	}
	info, err := s.Store.UploadIfMatch(ctx, s.manifestKey(kbID), manifestPath, expectedVersion)
	if err != nil {
		return "", err
	}
	return info.Version, nil
}

func (s *BlobStoreManifest) Delete(ctx context.Context, kbID string) error {
	err := s.Store.Delete(ctx, s.manifestKey(kbID))
	if err != nil {
		if errors.Is(err, blobstore.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("delete manifest for %s: %w", kbID, err)
	}
	return nil
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
