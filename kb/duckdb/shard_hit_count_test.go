package duckdb

// Pins the counting unit: one logical (query, shard) access records exactly
// one hit, even through the two-phase path (refs for every fanned-out shard
// plus hydrate for the merged winners).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/internal/budget"
	"github.com/mikills/minnow/internal/memlimit"
	kb "github.com/mikills/minnow/kb"
)

func TestOneHitPerShardQuery(t *testing.T) {
	ctx := context.Background()
	const dim = 8
	dir := t.TempDir()
	store := &kb.LocalBlobStore{Root: filepath.Join(dir, "blobs")}
	require.NoError(t, os.MkdirAll(store.Root, 0o755))
	cache := filepath.Join(dir, "cache")
	loader := kb.NewKB(store, cache)
	f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
		BlobStore: store, CacheDir: cache, Metrics: loader,
		MemoryLimit: "128MB", DuckDBThreads: 1, BuildThreads: 1,
		ExtensionDir: ResolveExtensionDir(), OfflineExt: true,
		Budget: budget.New(memlimit.Plan{}, false),
	}}
	t.Cleanup(func() { require.NoError(t, f.Close()) })

	var shards []kb.SnapshotShardMetadata
	for s := range 2 {
		path := filepath.Join(dir, fmt.Sprintf("shard-%d.duckdb", s))
		db, err := f.openConfiguredDB(ctx, path)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE docs
			(id TEXT, content TEXT, embedding FLOAT[%d], media_refs TEXT, metadata TEXT)`, dim))
		require.NoError(t, err)
		for i := range 16 {
			vector := make([]float32, dim)
			vector[(i+s)%dim] = 1 + float32(i)/100
			_, err := db.ExecContext(ctx, `INSERT INTO docs VALUES (?, ?, ?, NULL, ?)`,
				fmt.Sprintf("s%d-doc-%d", s, i), "content", vector, `{"parity":0}`)
			require.NoError(t, err)
		}
		require.NoError(t, db.Close())
		key := fmt.Sprintf("fixture/shard-%d.duckdb", s)
		_, err = store.UploadIfMatch(ctx, key, path, "")
		require.NoError(t, err)
		stat, err := os.Stat(path)
		require.NoError(t, err)
		checksum, err := kb.FileContentSHA256(ctx, path)
		require.NoError(t, err)
		shards = append(shards, kb.SnapshotShardMetadata{
			ShardID: fmt.Sprintf("shard-%d", s), Key: key,
			SizeBytes: stat.Size(), SHA256: checksum, VectorRows: 16,
		})
	}
	f.deps.ManifestStore = &stubManifestStore{doc: &kb.ManifestDocument{Manifest: kb.SnapshotShardManifest{
		FormatKind: DuckDBFormatKind, FormatVersion: DuckDBFormatVersion, Shards: shards,
	}}}

	var mu sync.Mutex
	counts := map[string]int{}
	f.deps.RecordShardHit = func(path string) {
		mu.Lock()
		defer mu.Unlock()
		counts[path]++
	}

	queryVec := make([]float32, dim)
	queryVec[0] = 1
	// Fanout 2 with localTopK > k forces refs + hydrate.
	results, err := f.queryTopKFromShards(ctx, shardTopKQuery{
		kbID: "fixture", queryVec: queryVec, k: 2, localTopK: 10,
		plan: shardQueryPlan{Shards: shards, Fanout: 2, Parallelism: 2},
	})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, counts, 2, "each fanned-out shard records exactly one hit")
	for path, n := range counts {
		require.Equal(t, 1, n, "shard %s recorded %d hits", path, n)
	}
}
