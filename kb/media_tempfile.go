package kb

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

// tempUpload holds a streamed upload on disk for size + checksum computation.
type tempUpload struct {
	path   string
	size   int64
	sha256 string
}

func (t *tempUpload) cleanup() {
	if t == nil || t.path == "" {
		return
	}
	_ = os.Remove(t.path)
}

// writeTempCapped streams body to a temp file, computing size + sha256 and
// rejecting uploads larger than maxBytes.
func writeTempCapped(body io.Reader, maxBytes int64) (*tempUpload, error) {
	f, err := os.CreateTemp("", "minnow-media-*")
	if err != nil {
		return nil, fmt.Errorf("media: tempfile: %w", err)
	}
	t := &tempUpload{path: f.Name()}

	hasher := sha256.New()
	limited := io.LimitReader(body, maxBytes+1)
	written, err := io.Copy(io.MultiWriter(f, hasher), limited)
	closeErr := f.Close()
	if err != nil {
		t.cleanup()
		return nil, fmt.Errorf("media: copy: %w", err)
	}
	if closeErr != nil {
		t.cleanup()
		return nil, fmt.Errorf("media: close tempfile: %w", closeErr)
	}
	if written > maxBytes {
		t.cleanup()
		return nil, errors.New("media: upload exceeds max size")
	}

	t.size = written
	t.sha256 = hex.EncodeToString(hasher.Sum(nil))
	return t, nil
}
