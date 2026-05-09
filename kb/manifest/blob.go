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
	Download(ctx context.Context, key string, dest string) error
	UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*blobstore.ObjectInfo, error)
	Delete(ctx context.Context, key string) error
}

type BlobStoreManifest struct {
	Store BlobStore
}

func (s *BlobStoreManifest) manifestKey(kbID string) string { return ShardManifestKey(kbID) }

func (s *BlobStoreManifest) Get(ctx context.Context, kbID string) (*Document, error) {
	key := s.manifestKey(kbID)
	tmpDir, err := os.MkdirTemp("", "minnow-manifest-get-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)
	manifestPath := filepath.Join(tmpDir, "manifest.json")
	info, err := s.downloadStableManifest(ctx, key, manifestPath)
	if err != nil {
		return nil, err
	}
	manifest, err := readManifestFile(manifestPath)
	if err != nil {
		return nil, err
	}
	return &Document{Manifest: manifest, Version: info.Version}, nil
}

func (s *BlobStoreManifest) downloadStableManifest(
	ctx context.Context,
	key string,
	path string,
) (*blobstore.ObjectInfo, error) {
	const maxAttempts = 1 << 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		info, err := s.downloadManifestAttempt(ctx, key, path)
		if err == nil {
			return info, nil
		}
		if !errors.Is(err, blobstore.ErrVersionMismatch) || attempt == maxAttempts-1 {
			return nil, err
		}
		if err := sleepWithContext(ctx, manifestReadBackoff(attempt)); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("manifest changed during read: %w", blobstore.ErrVersionMismatch)
}

func (s *BlobStoreManifest) downloadManifestAttempt(
	ctx context.Context,
	key string,
	path string,
) (*blobstore.ObjectInfo, error) {
	info, err := s.headManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	if err := s.Store.Download(ctx, key, path); err != nil {
		return nil, manifestStoreReadError(err)
	}
	latest, err := s.headManifest(ctx, key)
	if err != nil {
		return nil, err
	}
	if latest.Version != info.Version {
		return nil, fmt.Errorf("manifest changed during read: %w", blobstore.ErrVersionMismatch)
	}
	return info, nil
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

func readManifestFile(path string) (ShardManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ShardManifest{}, err
	}
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
