package blobstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// ObjectInfo describes a blob object.
type ObjectInfo struct {
	Key       string
	Version   string
	UpdatedAt time.Time
	Size      int64
}

// Store is the storage abstraction for KB snapshots.
type Store interface {
	Head(ctx context.Context, key string) (*ObjectInfo, error)
	DownloadBytes(ctx context.Context, key string) ([]byte, error)
	Download(ctx context.Context, key string, dest string) error
	UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*ObjectInfo, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

type Clock interface{ Now() time.Time }

var (
	ErrVersionMismatch = errors.New("blob version mismatch")
	ErrNotFound        = errors.New("blob not found")
)

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }
func nowFrom(c Clock) time.Time {
	if c == nil {
		return time.Now().UTC()
	}
	return c.Now()
}

func copyFileSync(src, dest string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func replaceFileWithCopy(src, dest string) error {
	tmpDest := fmt.Sprintf("%s.tmp-%d", dest, time.Now().UnixNano())
	if err := copyFileSync(src, tmpDest); err != nil {
		return err
	}
	defer os.Remove(tmpDest)
	return os.Rename(tmpDest, dest)
}

func FileContentSHA256(ctx context.Context, path string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
