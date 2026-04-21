package kb

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"math/rand/v2"

	"github.com/stretchr/testify/require"
)

// flakyUploadBlobStore wraps a BlobStore and fails the first N upload attempts
// with ErrBlobVersionMismatch. After the failures are exhausted, it delegates normally.
type flakyUploadBlobStore struct {
	base               BlobStore
	failUploadAttempts int
	mu                 sync.Mutex
}

func (f *flakyUploadBlobStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	return f.base.Head(ctx, key)
}

func (f *flakyUploadBlobStore) Download(ctx context.Context, key string, dest string) error {
	return f.base.Download(ctx, key, dest)
}

func (f *flakyUploadBlobStore) UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*BlobObjectInfo, error) {
	f.mu.Lock()
	if f.failUploadAttempts > 0 {
		f.failUploadAttempts--
		f.mu.Unlock()
		return nil, ErrBlobVersionMismatch
	}
	f.mu.Unlock()
	return f.base.UploadIfMatch(ctx, key, src, expectedVersion)
}

func (f *flakyUploadBlobStore) Delete(ctx context.Context, key string) error {
	return f.base.Delete(ctx, key)
}

func (f *flakyUploadBlobStore) List(ctx context.Context, prefix string) ([]BlobObjectInfo, error) {
	return f.base.List(ctx, prefix)
}

// manifestCASConflictStore wraps a BlobStore and fails manifest CAS uploads
// (non-empty expectedVersion) with ErrBlobVersionMismatch.
type manifestCASConflictStore struct {
	BlobStore
	manifestKey string
}

func (m *manifestCASConflictStore) UploadIfMatch(ctx context.Context, key string, src string, expectedVersion string) (*BlobObjectInfo, error) {
	if key == m.manifestKey && expectedVersion != "" {
		return nil, ErrBlobVersionMismatch
	}
	return m.BlobStore.UploadIfMatch(ctx, key, src, expectedVersion)
}

func (m *manifestCASConflictStore) Head(ctx context.Context, key string) (*BlobObjectInfo, error) {
	return m.BlobStore.Head(ctx, key)
}

func (m *manifestCASConflictStore) Download(ctx context.Context, key string, dest string) error {
	return m.BlobStore.Download(ctx, key, dest)
}

// flakyDeleteStore wraps a BlobStore and fails one delete for a specific key.
type flakyDeleteStore struct {
	BlobStore
	failKey string
	failed  bool
}

func newFlakyDeleteStore(inner BlobStore) *flakyDeleteStore {
	return &flakyDeleteStore{BlobStore: inner}
}

func (s *flakyDeleteStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if key == s.failKey && !s.failed {
		s.failed = true
		return fmt.Errorf("injected delete failure for %s", key)
	}
	local, ok := s.BlobStore.(*LocalBlobStore)
	if !ok {
		return fmt.Errorf("unsupported delete store")
	}
	return os.Remove(filepath.Join(local.Root, key))
}

// captureRetryObserver records MutationRetryStats for verification in tests.
type captureRetryObserver struct {
	mu    sync.Mutex
	stats []MutationRetryStats
}

func (o *captureRetryObserver) ObserveMutationRetry(s MutationRetryStats) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.stats = append(o.stats, s)
}

func (o *captureRetryObserver) Last() (MutationRetryStats, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.stats) == 0 {
		return MutationRetryStats{}, false
	}
	return o.stats[len(o.stats)-1], true
}

// countingEmbedder wraps an Embedder and counts Embed calls.
type countingEmbedder struct {
	base  Embedder
	mu    sync.Mutex
	calls int
}

func (e *countingEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	return e.base.Embed(ctx, input)
}

func (e *countingEmbedder) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// fixtureEmbedder produces deterministic embeddings based on FNV hash of input.
type fixtureEmbedder struct{ dim int }

func newFixtureEmbedder(dim int) Embedder { return fixtureEmbedder{dim: dim} }

func (e fixtureEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	if strings.TrimSpace(input) == "" {
		return nil, fmt.Errorf("input cannot be empty")
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(input))
	seed := uint64(hasher.Sum64())
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	vec := make([]float32, e.dim)
	for i := range vec {
		vec[i] = float32(rng.NormFloat64())
	}
	return normalizeVector(vec), nil
}

func normalizeVector(vec []float32) []float32 {
	var sumSq float32
	for _, v := range vec {
		sumSq += v * v
	}
	norm := float32(math.Sqrt(float64(sumSq)))
	if norm == 0 {
		return vec
	}
	normalized := make([]float32, len(vec))
	for i, v := range vec {
		normalized[i] = v / norm
	}
	return normalized
}

// seedManifest writes a manifest JSON to the blob store with the given shard metadata.
// This does not create any actual DuckDB files, only the manifest.
func seedManifest(t *testing.T, ctx context.Context, bs BlobStore, kbID string, shards []SnapshotShardMetadata) string {
	t.Helper()
	now := time.Now().UTC()
	manifest := SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         ShardManifestLayoutDuckDBs,
		FormatKind:     "mock",
		FormatVersion:  1,
		KBID:           kbID,
		CreatedAt:      now,
		TotalSizeBytes: 0,
		Shards:         shards,
	}
	for _, s := range shards {
		manifest.TotalSizeBytes += s.SizeBytes
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	require.NoError(t, err)
	manifestPath := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(manifestPath, data, 0o644))
	info, err := bs.UploadIfMatch(ctx, ShardManifestKey(kbID), manifestPath, "")
	require.NoError(t, err)
	return info.Version
}

// requireResultContainsID asserts results contain an entry with the given ID.
func requireResultContainsID(t *testing.T, results []ExpandedResult, id string) {
	t.Helper()
	for _, r := range results {
		if r.ID == id {
			return
		}
	}
	require.Failf(t, "assertion failed", "expected results to contain id=%q, got %+v", id, results)
}
