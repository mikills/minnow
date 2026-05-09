package fileingest

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"mime"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

func ID(raw string, index int) (string, error) {
	fileID := strings.TrimSpace(raw)
	if fileID == "" {
		return fmt.Sprintf("file-%03d-%s", index, randomHexString(3)), nil
	}
	if !idPattern.MatchString(fileID) {
		return "", fmt.Errorf("ingest: file_id %q must match [A-Za-z0-9_.-]+", fileID)
	}
	return fileID, nil
}

func GenerateDocumentID(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	base = strings.TrimSpace(base)
	if base == "" {
		base = "file"
	}
	return fmt.Sprintf("%s-%s", base, randomHexString(3))
}

func InferContentType(contentType string, cleanName string) string {
	ct := normaliseContentType(contentType)
	if ct != "application/octet-stream" {
		return ct
	}
	if guessed := mime.TypeByExtension(strings.ToLower(filepath.Ext(cleanName))); guessed != "" {
		return normaliseContentType(guessed)
	}
	return ct
}

func SupportedContentType(ct, filename string) bool {
	ct = normaliseContentType(ct)
	if idx := strings.Index(ct, ";"); idx >= 0 {
		ct = strings.TrimSpace(ct[:idx])
	}
	switch ct {
	case "application/pdf", "text/plain":
		return true
	case "", "application/octet-stream":
		ext := strings.ToLower(filepath.Ext(filename))
		return ext == ".pdf" || ext == ".txt"
	default:
		ext := strings.ToLower(filepath.Ext(filename))
		return ct == "text/markdown" && ext == ".txt"
	}
}

func randomHexString(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
