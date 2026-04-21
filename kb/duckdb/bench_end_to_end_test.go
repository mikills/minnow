package duckdb

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

// BenchmarkVectorQuery measures full QueryRag latency at several corpus sizes.
// Run with:
//
//	go test ./kb/duckdb/ -bench=BenchmarkVectorQuery -run=^$ -benchtime=1x -v -timeout=90m
func BenchmarkVectorQuery(b *testing.B) {
	cases := []struct {
		name       string
		corpusSize int
		vectorDim  int
		realCorpus bool
	}{
		{"10k_dim384", 10_000, 384, false},
		{"100k_dim384", 100_000, 384, false},
		{"1M_dim384", 1_000_000, 384, false},
		{"10k_dim768", 10_000, 768, false},
		{"100k_dim768", 100_000, 768, false},
		{"1M_dim768", 1_000_000, 768, false},
		{"10k_real_dim384", 10_000, 384, true},
		{"10k_real_dim768", 10_000, 768, true},
	}

	suiteStart := time.Now()
	totals := make([]struct {
		name  string
		total time.Duration
	}, 0, len(cases))

	for _, tc := range cases {
		tc := tc
		caseStart := time.Now()
		b.Run(tc.name, func(b *testing.B) {
			runVectorQueryBench(b, tc.corpusSize, tc.vectorDim, tc.realCorpus)
		})
		totals = append(totals, struct {
			name  string
			total time.Duration
		}{tc.name, time.Since(caseStart)})
	}

	b.Log("")
	b.Log("wall-time summary")
	for _, t := range totals {
		b.Logf("  %-14s %v", t.name, t.total)
	}
	b.Logf("  %-14s %v", "suite total", time.Since(suiteStart))
}

func runVectorQueryBench(b *testing.B, corpusSize, vectorDim int, realCorpus bool) {
	const (
		topK    = 10
		warmups = 100
		samples = 1_000
		kbID    = "bench-kb"
	)

	ctx := context.Background()

	blobRoot := filepath.Join(b.TempDir(), "blobs")
	require.NoError(b, os.MkdirAll(blobRoot, 0o755))
	cacheDir := filepath.Join(b.TempDir(), "cache")
	require.NoError(b, os.MkdirAll(cacheDir, 0o755))

	blobStore := &kb.LocalBlobStore{Root: blobRoot}
	manifestStore := &kb.BlobManifestStore{Store: blobStore}
	embedder, embedderLabel := pickEmbedder(b, vectorDim, realCorpus)

	loader := kb.NewKB(blobStore, cacheDir,
		kb.WithEmbedder(embedder),
		kb.WithManifestStore(manifestStore),
	)

	af, err := NewArtifactFormat(DuckDBArtifactDeps{
		BlobStore:      loader.BlobStore,
		ManifestStore:  loader.ManifestStore,
		CacheDir:       cacheDir,
		MemoryLimit:    "16GB",
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

	var docs []kb.Document
	var queryTexts []string
	if realCorpus {
		needed := corpusSize + warmups + samples
		all, err := loadRealCorpus(0)
		if err != nil {
			b.Skipf("real corpus unavailable: %v (run: go run ./scripts/fetch_corpus/)", err)
		}
		good := collectEmbeddableDocs(ctx, embedder, all, needed)
		if len(good) < needed {
			b.Skipf("real corpus has %d embeddable chunks after filter, need %d", len(good), needed)
		}
		docs = good[:corpusSize]
		corpusSize = len(docs)
		overflow := good[len(docs):]
		queryTexts = make([]string, 0, warmups+samples)
		for i := 0; i < warmups+samples; i++ {
			queryTexts = append(queryTexts, overflow[i%len(overflow)].Text)
		}
	} else {
		docs = make([]kb.Document, corpusSize)
		for i := range docs {
			docs[i] = kb.Document{ID: fmt.Sprintf("doc-%07d", i), Text: fmt.Sprintf("doc-%07d", i)}
		}
	}

	seedStart := time.Now()
	require.NoError(b, loader.UpsertDocsAndUpload(ctx, kbID, docs))
	seedElapsed := time.Since(seedStart)

	queryVecs := make([][]float32, warmups+samples)
	if realCorpus {
		for i, text := range queryTexts {
			vec, err := embedder.Embed(ctx, text)
			require.NoError(b, err)
			queryVecs[i] = vec
		}
	} else {
		rng := rand.New(rand.NewSource(42))
		for i := range queryVecs {
			vec := make([]float32, vectorDim)
			for j := range vec {
				vec[j] = float32(rng.NormFloat64())
			}
			queryVecs[i] = normalizeVec(vec)
		}
	}

	warmupStart := time.Now()
	for i := 0; i < warmups; i++ {
		_, err := af.QueryRag(ctx, kb.RagQueryRequest{
			KBID:     kbID,
			QueryVec: queryVecs[i],
			Options:  kb.RagQueryOptions{TopK: topK},
		})
		require.NoError(b, err)
	}
	warmupElapsed := time.Since(warmupStart)

	b.ResetTimer()

	durations := make([]time.Duration, 0, samples)
	runStart := time.Now()
	for i := 0; i < samples; i++ {
		qs := time.Now()
		_, err := af.QueryRag(ctx, kb.RagQueryRequest{
			KBID:     kbID,
			QueryVec: queryVecs[warmups+i],
			Options:  kb.RagQueryOptions{TopK: topK},
		})
		durations = append(durations, time.Since(qs))
		require.NoError(b, err)
	}
	runElapsed := time.Since(runStart)

	b.StopTimer()

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	pct := func(p int) time.Duration { return durations[(len(durations)*p)/100] }

	b.Logf("corpus=%d dim=%d topk=%d embedder=%s warmups=%d samples=%d",
		corpusSize, vectorDim, topK, embedderLabel, warmups, samples)
	b.Logf("seed:    %v  (%.1f docs/s)",
		seedElapsed, float64(corpusSize)/seedElapsed.Seconds())
	b.Logf("warmup:  %v", warmupElapsed)
	b.Logf("measure: %v  (%.1f qps)",
		runElapsed, float64(samples)/runElapsed.Seconds())
	b.Logf("latency  p50=%v  p90=%v  p99=%v  max=%v",
		pct(50), pct(90), pct(99), durations[len(durations)-1])
}

// collectEmbeddableDocs returns up to needed documents that the embedder
// accepts. Short/empty chunks are rejected by a cheap heuristic first; the
// embedder is only called on the remainder. Returns early once `needed` good
// docs have been found, so Ollama-backed embedders aren't asked to pre-embed
// the entire corpus just to filter a handful of bad chunks.
func collectEmbeddableDocs(ctx context.Context, embedder kb.Embedder, in []kb.Document, needed int) []kb.Document {
	out := make([]kb.Document, 0, needed)
	for _, d := range in {
		if !looksEmbeddable(d.Text) {
			continue
		}
		if _, err := embedder.Embed(ctx, d.Text); err != nil {
			continue
		}
		out = append(out, d)
		if len(out) >= needed {
			return out
		}
	}
	return out
}

// looksEmbeddable is a cheap pre-filter: require a minimum length and at least
// one run of 4+ consecutive letters. Keeps junk (pure whitespace, digits-only
// page numbers, short stopword blocks) out of the embedder path.
func looksEmbeddable(text string) bool {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < 40 {
		return false
	}
	run := 0
	for _, r := range trimmed {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			run++
			if run >= 4 {
				return true
			}
			continue
		}
		run = 0
	}
	return false
}

// pickEmbedder returns the embedder to use for a case plus a short label.
// Synthetic cases use the FNV-hash fixture embedder (uniform pseudo-random
// vectors). Real-corpus cases use a real embedder that produces clustered
// vectors: Ollama all-minilm at 384 dim, the in-repo LocalEmbedder at 768.
// If Ollama is selected but unreachable, the bench skips cleanly.
func pickEmbedder(b *testing.B, vectorDim int, realCorpus bool) (kb.Embedder, string) {
	if !realCorpus {
		return newFixtureEmbedder(vectorDim), fmt.Sprintf("fixture-fnv-%d", vectorDim)
	}
	if vectorDim == 768 {
		const model = "nomic-embed-text"
		emb := kb.NewOllamaEmbedder("http://localhost:11434", model)
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := emb.Ping(pingCtx); err != nil {
			b.Skipf("ollama unavailable: %v (start ollama and `ollama pull %s`)", err, model)
		}
		return emb, "ollama-" + strings.TrimSuffix(model, ":latest")
	}
	le, err := kb.NewLocalEmbedder(vectorDim)
	require.NoError(b, err)
	return le, fmt.Sprintf("local-subword-%d", vectorDim)
}

// loadRealCorpus reads JSONL files produced by scripts/fetch_corpus, returning
// up to limit Documents. Files are globbed from testdata/corpus/10k-filings/
// relative to the nearest ancestor with a go.mod file.
func loadRealCorpus(limit int) ([]kb.Document, error) {
	root, err := findRepoRoot()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, "testdata", "corpus", "10k-filings")
	matches, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no corpus files under %s", dir)
	}
	sort.Strings(matches)

	docs := make([]kb.Document, 0, limit)
	for _, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for sc.Scan() {
			var rec struct {
				ID   string `json:"id"`
				Text string `json:"text"`
			}
			if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
				f.Close()
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			docs = append(docs, kb.Document{ID: rec.ID, Text: rec.Text})
			if limit > 0 && len(docs) >= limit {
				f.Close()
				return docs, nil
			}
		}
		if err := sc.Err(); err != nil {
			f.Close()
			return nil, err
		}
		f.Close()
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("corpus files empty")
	}
	return docs, nil
}

func findRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from cwd upward")
		}
		dir = parent
	}
}
