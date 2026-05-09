package kb

import (
	"os"
	"path/filepath"
	"testing"
)

type TestHarness struct {
	t    *testing.T
	kbID string
	kb   *KB

	tempDir      string
	blobRoot     string
	cacheDir     string
	embedder     Embedder
	extraOptions []KBOption

	initialized bool
	cleanedUp   bool
}

func NewTestHarness(t *testing.T, kbID string) *TestHarness {
	return &TestHarness{
		t:    t,
		kbID: kbID,
	}
}

func (h *TestHarness) WithEmbedder(embedder Embedder) *TestHarness {
	h.embedder = embedder
	return h
}

func (h *TestHarness) WithOptions(opts ...KBOption) *TestHarness {
	h.extraOptions = append(h.extraOptions, opts...)
	return h
}

func (h *TestHarness) WithBlobRoot(dir string) *TestHarness {
	h.blobRoot = dir
	return h
}

func (h *TestHarness) Setup() *TestHarness {
	if h.initialized {
		h.t.Fatal("Harness already initialized")
	}

	if h.tempDir == "" {
		h.tempDir = h.t.TempDir()
	}
	if h.blobRoot == "" {
		h.blobRoot = filepath.Join(h.tempDir, "blobs")
	}
	h.cacheDir = filepath.Join(h.tempDir, "cache")

	if err := os.MkdirAll(h.blobRoot, 0o755); err != nil {
		h.t.Fatalf("Failed to create blob root: %v", err)
	}
	if err := os.MkdirAll(h.cacheDir, 0o755); err != nil {
		h.t.Fatalf("Failed to create cache dir: %v", err)
	}

	blobStore := &LocalBlobStore{Root: h.blobRoot}

	var opts []KBOption
	if h.embedder != nil {
		opts = append(opts, WithEmbedder(h.embedder))
	}
	opts = append(opts, h.extraOptions...)

	h.kb = NewKB(blobStore, h.cacheDir, opts...)

	h.initialized = true
	return h
}

func (h *TestHarness) Cleanup() {
	if h.cleanedUp {
		return
	}
	h.cleanedUp = true
}

func (h *TestHarness) KB() *KB {
	if !h.initialized {
		h.t.Fatal("Harness not initialized. Call Setup() first.")
	}
	return h.kb
}

func (h *TestHarness) BlobRoot() string {
	return h.blobRoot
}

func (h *TestHarness) CacheDir() string {
	return h.cacheDir
}

func SharedBlobRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "shared-blobs")
}
