package media

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
)

// TempUpload holds a streamed upload on disk for size + checksum computation.
type TempUpload struct {
	path   string
	size   int64
	sha256 string
}

func (t *TempUpload) Cleanup() {
	if t == nil || t.path == "" {
		return
	}
	if err := os.Remove(t.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Default().Warn("media temp upload remove failed", "path", t.path, "error", err)
	}
}

// writeTempCapped streams body to a temp file, computing size + sha256 and
// rejecting uploads larger than maxBytes.
func WriteTempCapped(body io.Reader, maxBytes int64) (*TempUpload, error) {
	f, err := os.CreateTemp("", "minnow-media-*")
	if err != nil {
		return nil, fmt.Errorf("media: tempfile: %w", err)
	}
	t := &TempUpload{path: f.Name()}

	hasher := sha256.New()
	limited := io.LimitReader(body, maxBytes+1)
	written, err := io.Copy(io.MultiWriter(f, hasher), limited)
	closeErr := f.Close()
	if err != nil {
		t.Cleanup()
		return nil, fmt.Errorf("media: copy: %w", err)
	}
	if closeErr != nil {
		t.Cleanup()
		return nil, fmt.Errorf("media: close tempfile: %w", closeErr)
	}
	if written > maxBytes {
		t.Cleanup()
		return nil, errors.New("media: upload exceeds max size")
	}

	t.size = written
	t.sha256 = hex.EncodeToString(hasher.Sum(nil))
	return t, nil
}

func (t *TempUpload) Path() string   { return t.path }
func (t *TempUpload) Size() int64    { return t.size }
func (t *TempUpload) SHA256() string { return t.sha256 }
