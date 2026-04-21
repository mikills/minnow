package kb

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"
	"unicode"
)

// MediaBlobKey returns the canonical blob key for a media object.
func MediaBlobKey(kbID, mediaID, filename string) string {
	return path.Join("kbs", kbID, "media", mediaID, filename)
}

// SanitizeMediaFilename strips path traversal, control chars, and clamps
// length.
func SanitizeMediaFilename(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("media: filename required")
	}
	// Reject explicit traversal attempts in the raw input before we
	// basename-normalise (so "../../etc/passwd" doesn't quietly become
	// "passwd").
	if strings.Contains(name, "..") {
		return "", errors.New("media: filename contains traversal")
	}
	// Take the basename to defend against absolute paths like /etc/passwd.
	name = path.Base(name)
	if name == "." || name == "/" || name == "" {
		return "", errors.New("media: filename invalid after sanitization")
	}
	// Strip control chars.
	clean := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == 0 {
			return -1
		}
		return r
	}, name)
	if clean == "" {
		return "", errors.New("media: filename empty after control char strip")
	}
	if len(clean) > 255 {
		clean = clean[:255]
	}
	return clean, nil
}

// newMediaID returns a sortable, URL-safe id built from unix-millis plus a
// short random suffix.
func newMediaID() string {
	now := time.Now().UTC()
	r := sha256.Sum256([]byte(fmt.Sprintf("%d-%d", now.UnixNano(), mediaIDCounter.next())))
	return fmt.Sprintf("med_%013d_%s", now.UnixMilli(), hex.EncodeToString(r[:6]))
}

type counter struct {
	mu sync.Mutex
	n  uint64
}

func (c *counter) next() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return c.n
}

var mediaIDCounter = &counter{}
