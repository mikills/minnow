package duckdb_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb"
	"github.com/mikills/minnow/kb/graphbuild"
	"github.com/mikills/minnow/kb/localembedder"
	"github.com/stretchr/testify/require"
)

func BenchmarkNormalDocumentIngestion(b *testing.B) {
	cases := map[string][]kb.Document{
		"small_100":   benchDocs("small", 100, 2),
		"small_1000":  benchDocs("small", 1000, 2),
		"medium_100":  benchDocs("medium", 100, 20),
		"large_10":    benchDocs("large", 10, 250),
		"mixed":       benchMixedDocs(),
		"graph_mixed": benchGraphDocs("graph", 100, 8),
	}
	for name, docs := range cases {
		b.Run(name+"/vector", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				loader := newNormalDocBenchKB(b, false)
				require.NoError(b, loader.UpsertDocsAndUpload(context.Background(), "kb", docs))
			}
		})
		b.Run(name+"/graph", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				loader := newNormalDocBenchKB(b, true)
				graphEnabled := true
				require.NoError(
					b,
					loader.UpsertDocsAndUploadWithOptions(
						context.Background(),
						"kb",
						docs,
						kb.UpsertDocsOptions{GraphEnabled: &graphEnabled},
					),
				)
			}
		})
	}
}

func BenchmarkNormalDocumentQuery(b *testing.B) {
	ctx := context.Background()
	docs := benchMixedDocs()
	for _, tc := range []struct {
		name string
		mode kb.SearchMode
	}{
		{name: "vector", mode: kb.SearchModeVector},
		{name: "graph", mode: kb.SearchModeGraph},
		{name: "adaptive", mode: kb.SearchModeAdaptive},
	} {
		b.Run(tc.name+"/warm", func(b *testing.B) {
			loader := newNormalDocBenchKB(b, true)
			graphEnabled := true
			require.NoError(
				b,
				loader.UpsertDocsAndUploadWithOptions(
					ctx,
					"kb",
					docs,
					kb.UpsertDocsOptions{GraphEnabled: &graphEnabled},
				),
			)
			queryVec, err := loader.Embed(ctx, "Alice and Bob search reliability at Acme")
			require.NoError(b, err)
			b.ReportAllocs()
			for b.Loop() {
				results, err := loader.Search(ctx, "kb", queryVec, &kb.SearchOptions{Mode: tc.mode, TopK: 10})
				require.NoError(b, err)
				require.NotEmpty(b, results)
			}
		})
		b.Run(tc.name+"/cold", func(b *testing.B) {
			seed := newNormalDocBenchKB(b, true)
			graphEnabled := true
			require.NoError(
				b,
				seed.UpsertDocsAndUploadWithOptions(ctx, "kb", docs, kb.UpsertDocsOptions{GraphEnabled: &graphEnabled}),
			)
			queryVec, err := seed.Embed(ctx, "Alice and Bob search reliability at Acme")
			require.NoError(b, err)
			blobRoot := seedBlobRoot(seed)
			b.ReportAllocs()
			for b.Loop() {
				loader := newNormalDocBenchKBWithBlobRoot(b, blobRoot, true)
				results, err := loader.Search(ctx, "kb", queryVec, &kb.SearchOptions{Mode: tc.mode, TopK: 10})
				require.NoError(b, err)
				require.NotEmpty(b, results)
			}
		})
	}
}

func newNormalDocBenchKB(b *testing.B, graph bool) *kb.KB {
	return newNormalDocBenchKBWithBlobRoot(b, filepath.Join(b.TempDir(), "blobs"), graph)
}

func newNormalDocBenchKBWithBlobRoot(b *testing.B, blobRoot string, graph bool) *kb.KB {
	b.Helper()
	require.NoError(b, os.MkdirAll(blobRoot, 0o755))
	cacheDir := filepath.Join(b.TempDir(), "cache")
	embedder, err := localembedder.New(64, kb.ErrInvalidEmbeddingDimension)
	require.NoError(b, err)
	blobStore := &kb.LocalBlobStore{Root: blobRoot}
	opts := []kb.KBOption{kb.WithEmbedder(embedder), kb.WithManifestStore(&kb.BlobManifestStore{Store: blobStore})}
	if graph {
		opts = append(
			opts,
			kb.WithGraphBuilder(
				&kb.GraphBuilder{Chunker: kb.TextChunker{ChunkSize: 500}, Grapher: benchGrapher{}, BatchSize: 32},
			),
		)
	}
	loader := kb.NewKB(blobStore, cacheDir, opts...)
	af, err := duckdb.NewArtifactFormat(duckdb.DuckDBArtifactDeps{
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

func seedBlobRoot(loader *kb.KB) string {
	if local, ok := loader.BlobStore.(*kb.LocalBlobStore); ok {
		return local.Root
	}
	return ""
}

type benchGrapher struct{}

func (benchGrapher) Extract(_ context.Context, chunks []graphbuild.Chunk) (*graphbuild.Extraction, error) {
	out := &graphbuild.Extraction{}
	for _, chunk := range chunks {
		entities := []string{"Alice", "Bob", "Acme"}
		for _, entity := range entities {
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

func benchDocs(prefix string, count int, paragraphs int) []kb.Document {
	docs := make([]kb.Document, 0, count)
	for i := range count {
		docs = append(
			docs,
			kb.Document{ID: fmt.Sprintf("%s-doc-%04d", prefix, i), Text: benchDocText(prefix, i, paragraphs)},
		)
	}
	return docs
}

func benchGraphDocs(prefix string, count int, paragraphs int) []kb.Document {
	docs := benchDocs(prefix, count, paragraphs)
	for i := range docs {
		docs[i].Text += "\nAlice works with Bob at Acme. Bob coordinates Acme search reliability projects."
	}
	return docs
}

func benchMixedDocs() []kb.Document {
	docs := benchDocs("mixed-small", 100, 2)
	docs = append(docs, benchDocs("mixed-medium", 40, 20)...)
	docs = append(docs, benchDocs("mixed-large", 5, 250)...)
	return docs
}

func benchDocText(prefix string, idx int, paragraphs int) string {
	var b strings.Builder
	for p := range paragraphs {
		fmt.Fprintf(
			&b,
			"%s document %d paragraph %d covers ingestion throughput, query latency, manifest safety, cache warmth, and graph relationships. "+
				"Alice and Bob discuss Acme reliability work, vector search, retries, leases, and deterministic testing.\n\n",
			prefix,
			idx,
			p,
		)
	}
	return b.String()
}
