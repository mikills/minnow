package duckdb

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/graphbuild"
	"github.com/mikills/minnow/kb/localembedder"
	"github.com/stretchr/testify/require"
)

func BenchmarkFanoutQueryMaterialization(b *testing.B) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		mode     kb.SearchMode
		graph    bool
		twoPhase bool
	}{
		{name: "vector/one_phase", mode: kb.SearchModeVector, graph: true},
		{name: "vector/two_phase", mode: kb.SearchModeVector, graph: true, twoPhase: true},
		{name: "graph/one_phase", mode: kb.SearchModeGraph, graph: true},
		{name: "graph/two_phase", mode: kb.SearchModeGraph, graph: true, twoPhase: true},
		{name: "adaptive/one_phase", mode: kb.SearchModeAdaptive, graph: true},
		{name: "adaptive/two_phase", mode: kb.SearchModeAdaptive, graph: true, twoPhase: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkFanoutQueryMaterializationCase(b, ctx, tc.mode, tc.graph, tc.twoPhase)
		})
	}
}

func benchmarkFanoutQueryMaterializationCase(
	b *testing.B,
	ctx context.Context,
	mode kb.SearchMode,
	graph bool,
	twoPhase bool,
) {
	b.Helper()
	loader := newFanoutBenchKB(b, graph)
	docs := fanoutBenchDocs(240, 16)
	graphEnabled := true
	require.NoError(
		b,
		loader.UpsertDocsAndUploadWithOptions(ctx, "kb", docs, kb.UpsertDocsOptions{GraphEnabled: &graphEnabled}),
	)
	manifestDoc, err := loader.ManifestStore.Get(ctx, "kb")
	require.NoError(b, err)
	require.Greater(b, len(manifestDoc.Manifest.Shards), 2)
	queryVec, err := loader.Embed(ctx, "Alice Bob Acme permissions search reliability")
	require.NoError(b, err)
	previous := twoPhaseMaterializationEnabled
	twoPhaseMaterializationEnabled = twoPhase
	b.ReportAllocs()
	for b.Loop() {
		results, err := loader.Search(ctx, "kb", queryVec, &kb.SearchOptions{Mode: mode, TopK: 10})
		require.NoError(b, err)
		require.NotEmpty(b, results)
	}
	twoPhaseMaterializationEnabled = previous
}

func newFanoutBenchKB(b *testing.B, graphEnabled bool) *kb.KB {
	b.Helper()
	blobRoot := filepath.Join(b.TempDir(), "blobs")
	require.NoError(b, os.MkdirAll(blobRoot, 0o755))
	cacheDir := filepath.Join(b.TempDir(), "cache")
	embedder, err := localembedder.New(64, kb.ErrInvalidEmbeddingDimension)
	require.NoError(b, err)
	blobStore := &kb.LocalBlobStore{Root: blobRoot}
	policy := kb.ShardingPolicy{
		ShardTriggerBytes:       1,
		ShardTriggerVectorRows:  1,
		TargetShardBytes:        8192,
		MaxVectorRowsPerShard:   24,
		QueryShardFanout:        6,
		QueryShardParallelism:   4,
		QueryShardLocalTopKMult: 4,
		SmallKBMaxShards:        1,
	}
	opts := []kb.KBOption{
		kb.WithEmbedder(embedder),
		kb.WithManifestStore(&kb.BlobManifestStore{Store: blobStore}),
		kb.WithShardingPolicy(policy),
	}
	if graphEnabled {
		opts = append(
			opts,
			kb.WithGraphBuilder(
				&kb.GraphBuilder{Chunker: kb.TextChunker{ChunkSize: 500}, Grapher: fanoutBenchGrapher{}, BatchSize: 32},
			),
		)
	}
	loader := kb.NewKB(blobStore, cacheDir, opts...)
	af, err := NewArtifactFormat(DuckDBArtifactDeps{
		BlobStore:      loader.BlobStore,
		ManifestStore:  loader.ManifestStore,
		CacheDir:       cacheDir,
		MemoryLimit:    "4GB",
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
	require.NoError(b, err)
	require.NoError(b, loader.RegisterFormat(af))
	return loader
}

func fanoutBenchDocs(count int, paragraphs int) []kb.Document {
	docs := make([]kb.Document, 0, count)
	for i := range count {
		docs = append(docs, kb.Document{
			ID:        fmt.Sprintf("fanout-doc-%04d", i),
			Text:      fanoutBenchText(i, paragraphs),
			MediaRefs: fanoutBenchMediaRefs(i),
		})
	}
	return docs
}

func fanoutBenchText(idx int, paragraphs int) string {
	var b strings.Builder
	for p := range paragraphs {
		fmt.Fprintf(
			&b,
			"fanout document %d paragraph %d has a deliberately wide payload about Alice, Bob, Acme, permissions, tags, source metadata, search reliability, graph expansion, materialization, cache warmup, shard fanout, and query ranking. "+
				"The payload is intentionally repetitive so benchmarks include content hydration costs for discarded local candidates as well as final winners.\n\n",
			idx,
			p,
		)
	}
	return b.String()
}

func fanoutBenchMediaRefs(idx int) []kb.ChunkMediaRef {
	return []kb.ChunkMediaRef{
		{
			MediaID: fmt.Sprintf("media-%04d", idx),
			BlobKey: fmt.Sprintf("media/%04d.bin", idx),
			Role:    "source",
			Label:   "benchmark source payload",
			Metadata: map[string]any{
				"tag":        "fanout",
				"permission": "team-search",
				"ordinal":    idx,
			},
		},
	}
}

type fanoutBenchGrapher struct{}

func (fanoutBenchGrapher) Extract(_ context.Context, chunks []graphbuild.Chunk) (*graphbuild.Extraction, error) {
	out := &graphbuild.Extraction{}
	for _, chunk := range chunks {
		for _, entity := range []string{"Alice", "Bob", "Acme"} {
			if strings.Contains(chunk.Text, entity) {
				out.Entities = append(out.Entities, graphbuild.EntityCandidate{Name: entity, ChunkID: chunk.ChunkID})
			}
		}
		if strings.Contains(chunk.Text, "Alice") && strings.Contains(chunk.Text, "Bob") {
			out.Edges = append(
				out.Edges,
				graphbuild.EdgeCandidate{
					Src:     "Alice",
					Dst:     "Bob",
					RelType: "works_with",
					Weight:  1,
					ChunkID: chunk.ChunkID,
				},
			)
		}
		if strings.Contains(chunk.Text, "Bob") && strings.Contains(chunk.Text, "Acme") {
			out.Edges = append(
				out.Edges,
				graphbuild.EdgeCandidate{
					Src:     "Bob",
					Dst:     "Acme",
					RelType: "works_at",
					Weight:  1,
					ChunkID: chunk.ChunkID,
				},
			)
		}
	}
	return out, nil
}
