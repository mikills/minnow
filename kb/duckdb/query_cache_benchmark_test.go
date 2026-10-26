package duckdb

// This exact fixture is also compiled against the pre-optimization revision in
// CI. Use only APIs present there; never compare different benchmark sources.
import (
	"context"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	duckdbdriver "github.com/duckdb/duckdb-go/v2"
	"github.com/mikills/minnow/internal/budget"
	"github.com/mikills/minnow/internal/memlimit"
	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/search"
	"github.com/stretchr/testify/require"
)

func BenchmarkWarmShardQuery(b *testing.B) {
	if os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("RUNNER_ENVIRONMENT") != "github-hosted" || runtime.GOOS != "linux" {
		b.Skip("performance measurement is restricted to GitHub-hosted Linux runners")
	}
	for _, tc := range []struct {
		name, mode string
		dim        int
	}{
		{"full_32", "full", 32}, {"full_512", "full", 512},
		{"refs_512", "refs", 512}, {"filtered_512", "filtered", 512},
		{"scoped_512", "scoped", 512}, {"bm25_512", "bm25", 512},
		{"churn_512", "churn", 512},
	} {
		b.Run(tc.name, func(b *testing.B) {
			f, shard := warmShardBenchmarkFixture(b, tc.dim)
			ctx := context.Background()
			vectors := [][]float32{warmShardBenchVector(tc.dim, 0), warmShardBenchVector(tc.dim, 1)}
			run := func(i int) {
				axis := i % 2
				input := shardQueryInput{queryVec: vectors[axis], k: 10}
				if tc.mode == "filtered" {
					input.filter = &search.FilterExpr{Field: "parity", Op: search.FilterOpEq, Value: float64(axis)}
				}
				if tc.mode == "scoped" {
					input.documentIDs = []string{"doc-0", "doc-1"}
				}
				if tc.mode == "churn" {
					input.k = 1 + i%10
				}
				var results []kb.QueryResult
				var err error
				switch tc.mode {
				case "refs":
					var refs []rankedDocRef
					refs, err = f.querySingleShardTopKRefs(ctx, "fixture", shard, input)
					if err == nil {
						results, err = f.hydrateShardRankedResults(ctx, "fixture", shard, refs)
					}
				case "bm25":
					results, err = f.searchBM25AllShards(ctx, "fixture", "search", 10, nil, nil)
				default:
					results, err = f.querySingleShardTopK(ctx, "fixture", shard, input)
				}
				require.NoError(b, err)
				require.NotEmpty(b, results)
				if tc.mode != "bm25" {
					require.Equal(b, fmt.Sprintf("doc-%d", axis), results[0].ID, "changing bindings must not return a stale result")
				}
			}
			for i := range 10 {
				run(i)
			}
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				run(i)
				i++
			}
		})
	}
}

func warmShardBenchmarkFixture(b *testing.B, dim int) (*DuckDBArtifactFormat, kb.SnapshotShardMetadata) {
	b.Helper()
	ctx := context.Background()
	dir := b.TempDir()
	store := &kb.LocalBlobStore{Root: filepath.Join(dir, "blobs")}
	require.NoError(b, os.MkdirAll(store.Root, 0o755))
	cache := filepath.Join(dir, "cache")
	loader := kb.NewKB(store, cache)
	f := &DuckDBArtifactFormat{deps: DuckDBArtifactDeps{
		BlobStore: store, CacheDir: cache, Metrics: loader,
		MemoryLimit: "128MB", DuckDBThreads: 1, BuildThreads: 1,
		ExtensionDir: ResolveExtensionDir(), OfflineExt: true,
		Budget: budget.New(memlimit.Plan{}, false),
	}}
	b.Cleanup(func() { require.NoError(b, f.Close()) })
	path := filepath.Join(dir, "source.duckdb")
	db, err := f.openConfiguredDB(ctx, path)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, db.Close()) })
	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE docs
		(id TEXT, content TEXT, embedding FLOAT[%d], media_refs TEXT, metadata TEXT)`, dim))
	require.NoError(b, err)
	conn, err := db.Conn(ctx)
	require.NoError(b, err)
	err = conn.Raw(func(raw any) error {
		appender, err := duckdbdriver.NewAppenderFromConn(raw.(driver.Conn), "", "docs")
		if err != nil {
			return err
		}
		for i := range 1024 {
			vector := warmShardBenchVector(dim, i)
			if err := appender.AppendRow(fmt.Sprintf("doc-%d", i), "document search content", vector, nil, fmt.Sprintf(`{"parity":%d}`, i%2)); err != nil {
				_ = appender.Close()
				return err
			}
		}
		return appender.Close()
	})
	require.NoError(b, conn.Close())
	require.NoError(b, err)
	_, err = db.ExecContext(ctx, `CREATE INDEX docs_vec_idx ON docs USING HNSW (embedding);
		PRAGMA create_fts_index('docs', 'id', 'content', stemmer='none', stopwords='none')`)
	require.NoError(b, err)
	// Verify index eligibility for both bound inputs, outside measurement. This
	// is not a claim that every internal prepared execution has an identical plan.
	for axis := range 2 {
		vector := warmShardBenchVector(dim, axis)
		rows, err := db.QueryContext(ctx, fmt.Sprintf("EXPLAIN SELECT id FROM docs ORDER BY array_distance(embedding, $1::FLOAT[%d]) LIMIT 10", dim), vector)
		require.NoError(b, err)
		var plan strings.Builder
		for rows.Next() {
			var key, value string
			require.NoError(b, rows.Scan(&key, &value))
			plan.WriteString(value)
		}
		require.NoError(b, rows.Err())
		require.NoError(b, rows.Close())
		require.Contains(b, strings.ReplaceAll(strings.ToUpper(plan.String()), " ", "_"), "HNSW_INDEX_SCAN")
	}
	require.NoError(b, CheckpointAndCloseDB(ctx, db, "close query benchmark fixture"))
	info, err := store.UploadIfMatch(ctx, "fixture/shard.duckdb", path, "")
	require.NoError(b, err)
	stat, err := os.Stat(path)
	require.NoError(b, err)
	checksum, err := kb.FileContentSHA256(ctx, path)
	require.NoError(b, err)
	shard := kb.SnapshotShardMetadata{ShardID: "fixture", Key: "fixture/shard.duckdb",
		Version: info.Version, SizeBytes: stat.Size(), SHA256: checksum, VectorRows: 1024}
	f.deps.ManifestStore = &stubManifestStore{doc: &kb.ManifestDocument{Manifest: kb.SnapshotShardManifest{
		FormatKind: DuckDBFormatKind, FormatVersion: DuckDBFormatVersion, Shards: []kb.SnapshotShardMetadata{shard},
	}}}
	return f, shard
}

func warmShardBenchVector(dim, document int) []float32 {
	vector := make([]float32, dim)
	state := uint32(document + 1)
	for i := range vector {
		state = state*1664525 + 1013904223
		vector[i] = float32(int32(state)) / 2147483648
	}
	return vector
}
