package duckdb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
)

func mustLocalEmbedder(t *testing.T, dim int) kb.Embedder {
	t.Helper()
	embedder, err := kb.NewLocalEmbedder(dim)
	require.NoError(t, err)
	return embedder
}

func registerDuckDBFormatOnHarness(t *testing.T, h *kb.TestHarness) {
	t.Helper()
	loader := h.KB()
	af, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
		BlobStore:      loader.BlobStore,
		ManifestStore:  loader.ManifestStore,
		CacheDir:       h.CacheDir(),
		MemoryLimit:    "128MB",
		ShardingPolicy: loader.ShardingPolicy,
		Embed:          loader.Embed,
		GraphBuilder:   func() *kb.GraphBuilder { return loader.GraphBuilder },
		EvictCacheIfNeeded: func(ctx context.Context, protectKBID string) error {
			return loader.EvictCacheIfNeeded(ctx, protectKBID)
		},
		LockFor: loader.LockFor,
		AcquireWriteLease: func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
			return loader.AcquireWriteLease(ctx, kbID)
		},
		EnqueueReplacedShardsForGC: loader.EnqueueReplacedShardsForGC,
		Metrics:                    loader,
	})
	require.NoError(t, err)
	require.NoError(t, loader.RegisterFormat(af))
}

type noopMetrics struct{}

func (noopMetrics) RecordShardCount(string, int)        {}
func (noopMetrics) RecordShardExecutionFailure(string)  {}
func (noopMetrics) RecordShardFanout(string, int, bool) {}
func (noopMetrics) RecordShardExecution(string, int)    {}
func (noopMetrics) RecordShardCacheAccess(string, bool) {}

func noopAcquireWriteLease() func(context.Context, string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
	mgr := kb.NewInMemoryWriteLeaseManager()
	return func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
		lease, err := mgr.Acquire(ctx, kbID, 30*time.Second)
		return mgr, lease, err
	}
}

func uploadTestShardManifest(t *testing.T, loader *kb.KB, kbID string, shardCount int) {
	t.Helper()
	manifest := kb.SnapshotShardManifest{
		SchemaVersion:  1,
		Layout:         kb.ShardManifestLayoutDuckDBs,
		FormatKind:     duckdb.DuckDBFormatKind,
		FormatVersion:  duckdb.DuckDBFormatVersion,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: int64(shardCount),
		Shards:         make([]kb.SnapshotShardMetadata, 0, shardCount),
	}
	for i := range shardCount {
		manifest.Shards = append(
			manifest.Shards,
			kb.SnapshotShardMetadata{
				ShardID:    fmt.Sprintf("shard-%03d", i),
				Key:        fmt.Sprintf("%s/shard-%03d.duckdb", kbID, i),
				SizeBytes:  1,
				VectorRows: 1,
				CreatedAt:  time.Now().UTC(),
			},
		)
	}
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(path, data, 0o644))
	_, err = loader.BlobStore.UploadIfMatch(context.Background(), kb.ShardManifestKey(kbID), path, "")
	require.NoError(t, err)
}

func requireDuckDBFormat(t *testing.T, loader *kb.KB) *duckdb.DuckDBArtifactFormat {
	t.Helper()
	format, ok := loader.DefaultFormat().(*duckdb.DuckDBArtifactFormat)
	require.True(t, ok, "expected *DuckDBArtifactFormat, got %T", loader.DefaultFormat())
	return format
}
