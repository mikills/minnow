package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/kbcore/kb"
)

func openConfiguredDBForTest(ctx context.Context, dbPath, memLimit, extensionDir string, offlineExt bool) (*sql.DB, error) {
	f := &DuckDBArtifactFormat{
		deps: DuckDBArtifactDeps{
			MemoryLimit:  memLimit,
			ExtensionDir: extensionDir,
			OfflineExt:   offlineExt,
		},
	}
	return f.openConfiguredDB(ctx, dbPath)
}

func TestDuckDBCorrectness(t *testing.T) {
	t.Run("concurrent_reads_finance_fixture", testE2EConcurrentReadsFinanceFixture)
	t.Run("concurrent_writes_recipe_fixture", testE2EConcurrentWritesRecipeFixture)
	t.Run("query_recall_against_bruteforce", testMultiTenantQueryRecallAgainstBruteForce)
	t.Run("write_visibility_across_readers", testMultiTenantWriteVisibilityAcrossReaders)
}

func testE2EConcurrentReadsFinanceFixture(t *testing.T) {
	ctx := context.Background()
	sharedBlobRoot := kb.SharedBlobRoot(t)
	embedder := newFixtureEmbedder(64)
	writer := kb.NewTestHarness(t, "unused").WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
	t.Cleanup(writer.Cleanup)
	registerFormatOnHarness(t, writer)
	loader := writer.KB()

	financeTenant := "tenant-finance"
	recipeTenant := "tenant-recipe"
	financeDocs := buildChunkedDocuments("finance", readFixtureDocument(t, "finance-aapl-10k.txt"), 110, 80)
	require.Greater(t, len(financeDocs), 5)
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, financeTenant, financeDocs))

	recipeLines := nonEmptyLines(readFixtureDocument(t, "conv-recipe.txt"))
	require.Greater(t, len(recipeLines), 3)
	recipeSeedDocs := make([]kb.Document, 0, len(recipeLines))
	for i, line := range recipeLines {
		recipeSeedDocs = append(recipeSeedDocs, kb.Document{ID: fmt.Sprintf("recipe-%03d", i), Text: line})
	}
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, recipeTenant, recipeSeedDocs))

	queries := []struct {
		text   string
		anchor string
	}{
		{text: "Apple Intelligence and generative models", anchor: "intelligence"},
		{text: "iPhone 16 Pro Max and product announcements", anchor: "iphone"},
		{text: "fiscal year macroeconomic conditions and inflation", anchor: "fiscal"},
	}

	queryVecs := make([][]float32, 0, len(queries))
	for _, q := range queries {
		vec, err := loader.Embed(ctx, q.text)
		require.NoError(t, err)
		queryVecs = append(queryVecs, vec)
	}

	readerHarnesses := make([]*kb.TestHarness, 0, 4)
	for i := 0; i < 4; i++ {
		rh := kb.NewTestHarness(t, fmt.Sprintf("reader-%d", i)).WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
		t.Cleanup(rh.Cleanup)
		registerFormatOnHarness(t, rh)
		readerHarnesses = append(readerHarnesses, rh)
		_, err := rh.KB().Search(ctx, financeTenant, queryVecs[0], &kb.SearchOptions{TopK: 1})
		require.NoError(t, err)
	}

	readerErrCh := make(chan error, 32)
	var readerWG sync.WaitGroup
	readerWorkers := len(readerHarnesses)
	iterationsPerReader := 80
	for worker := 0; worker < readerWorkers; worker++ {
		worker := worker
		readerKB := readerHarnesses[worker].KB()
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for i := 0; i < iterationsPerReader; i++ {
				qIdx := (worker + i) % len(queryVecs)
				results, err := readerKB.Search(ctx, financeTenant, queryVecs[qIdx], &kb.SearchOptions{TopK: 20})
				if err != nil {
					readerErrCh <- err
					return
				}
				if len(results) == 0 {
					readerErrCh <- fmt.Errorf("empty result set for finance tenant")
					return
				}
				for _, r := range results {
					if !strings.HasPrefix(r.ID, "finance-") {
						readerErrCh <- fmt.Errorf("cross-tenant result in finance query: id=%s", r.ID)
						return
					}
				}
				_ = queries[qIdx]
			}
		}()
	}
	readerWG.Wait()
	close(readerErrCh)
	for err := range readerErrCh {
		require.NoError(t, err)
	}
}

func testE2EConcurrentWritesRecipeFixture(t *testing.T) {
	ctx := context.Background()
	sharedBlobRoot := kb.SharedBlobRoot(t)
	embedder := newFixtureEmbedder(64)

	writer := kb.NewTestHarness(t, "unused").WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
	t.Cleanup(writer.Cleanup)
	registerFormatOnHarness(t, writer)
	writerKB := writer.KB()
	tenantID := "tenant-recipe-writes"

	recipeLines := nonEmptyLines(readFixtureDocument(t, "conv-recipe.txt"))
	require.Greater(t, len(recipeLines), 5)
	require.NoError(t, writerKB.UpsertDocsAndUpload(ctx, tenantID, []kb.Document{{
		ID:   "recipe-seed",
		Text: recipeLines[0],
	}}))

	readerQueries := []string{
		"carbonara eggs scrambling creamy sauce",
		"guanciale pecorino romano and bacon",
		"rigatoni spaghetti pasta water",
	}
	readerHarnesses := make([]*kb.TestHarness, 0, 4)
	for i := 0; i < 4; i++ {
		rh := kb.NewTestHarness(t, fmt.Sprintf("recipe-reader-%d", i)).WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
		t.Cleanup(rh.Cleanup)
		registerFormatOnHarness(t, rh)
		readerHarnesses = append(readerHarnesses, rh)
	}

	queryVecs := make([][]float32, 0, len(readerQueries))
	for _, q := range readerQueries {
		v, err := writerKB.Embed(ctx, q)
		require.NoError(t, err)
		queryVecs = append(queryVecs, v)
	}
	for _, rh := range readerHarnesses {
		_, err := rh.KB().Search(ctx, tenantID, queryVecs[0], &kb.SearchOptions{TopK: 1})
		require.NoError(t, err)
	}

	var mu sync.Mutex
	writtenDocs := make([]kb.Document, 0, 128)
	readerErrCh := make(chan error, 32)
	stopReaders := make(chan struct{})

	var readerWG sync.WaitGroup
	for worker := 0; worker < len(readerHarnesses); worker++ {
		worker := worker
		readerKB := readerHarnesses[worker].KB()
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			iter := 0
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				vec := queryVecs[(worker+iter)%len(queryVecs)]
				iter++
				res, err := readerKB.Search(ctx, tenantID, vec, &kb.SearchOptions{TopK: 5})
				if err != nil {
					readerErrCh <- err
					return
				}
				if len(res) == 0 {
					readerErrCh <- fmt.Errorf("empty result set while reading recipe tenant")
					return
				}
				for _, r := range res {
					if !strings.HasPrefix(r.ID, "recipe-") {
						readerErrCh <- fmt.Errorf("cross-tenant result while writing recipe docs: id=%s", r.ID)
						return
					}
				}
			}
		}()
	}

	writerWorkers := 3
	writesPerWorker := 18
	var writerWG sync.WaitGroup
	for worker := 0; worker < writerWorkers; worker++ {
		worker := worker
		writerWG.Add(1)
		go func() {
			defer writerWG.Done()
			for i := 0; i < writesPerWorker; i++ {
				line := recipeLines[(worker+i)%len(recipeLines)]
				doc := kb.Document{
					ID:   fmt.Sprintf("recipe-w%d-%03d", worker, i),
					Text: fmt.Sprintf("%s (writer=%d seq=%d)", line, worker, i),
				}
				if err := writerKB.UpsertDocsAndUpload(ctx, tenantID, []kb.Document{doc}); err != nil {
					readerErrCh <- err
					return
				}
				mu.Lock()
				writtenDocs = append(writtenDocs, doc)
				mu.Unlock()
			}
		}()
	}

	writerWG.Wait()
	close(stopReaders)
	readerWG.Wait()
	close(readerErrCh)
	for err := range readerErrCh {
		require.NoError(t, err)
	}

	mu.Lock()
	docs := append([]kb.Document(nil), writtenDocs...)
	mu.Unlock()
	require.Len(t, docs, writerWorkers*writesPerWorker)

	for i := 0; i < len(docs); i += 7 {
		require.NoError(t, waitForDocVisible(ctx, readerHarnesses[0].KB(), tenantID, docs[i].Text, docs[i].ID, 4*time.Second))
	}
}

func testMultiTenantQueryRecallAgainstBruteForce(t *testing.T) {
	ctx := context.Background()
	embedder := newFixtureEmbedder(64)
	h := kb.NewTestHarness(t, "unused").WithEmbedder(embedder).Setup()
	t.Cleanup(h.Cleanup)
	registerFormatOnHarness(t, h)
	loader := h.KB()

	tenantID := "tenant-recall"
	docs := buildChunkedDocuments("finance", readFixtureDocument(t, "finance-aapl-10k.txt"), 90, 70)
	require.Greater(t, len(docs), 5)
	require.NoError(t, loader.UpsertDocsAndUpload(ctx, tenantID, docs))

	docEmbeddings := make(map[string][]float32, len(docs))
	for _, d := range docs {
		v, err := loader.Embed(ctx, d.Text)
		require.NoError(t, err)
		docEmbeddings[d.ID] = v
	}

	const k = 5
	queryTexts := make([]string, 0, 24)
	for i := 0; i < 6 && i < len(docs); i++ {
		queryTexts = append(queryTexts, docs[i].Text)
	}
	queryTexts = append(queryTexts,
		"Apple Intelligence generative models",
		"MacBook Air 15-in product announcements",
		"fiscal year and macroeconomic conditions",
	)
	if len(queryTexts) < 8 {
		queryTexts = append(queryTexts, docs[len(docs)-1].Text)
	}

	var totalRecall float64
	for _, queryText := range queryTexts {
		qVec, err := loader.Embed(ctx, queryText)
		require.NoError(t, err)

		actual, err := loader.Search(ctx, tenantID, qVec, &kb.SearchOptions{TopK: k})
		require.NoError(t, err)
		require.NotEmpty(t, actual)

		expectedIDs := bruteForceTopK(docEmbeddings, qVec, k)
		actualIDs := make([]string, 0, len(actual))
		for _, r := range actual {
			actualIDs = append(actualIDs, r.ID)
		}

		overlap := overlapCount(expectedIDs, actualIDs)
		totalRecall += float64(overlap) / float64(k)
	}

	avgRecall := totalRecall / float64(len(queryTexts))
	assert.GreaterOrEqual(t, avgRecall, 0.85, "recall@%d below threshold", k)
}

func testMultiTenantWriteVisibilityAcrossReaders(t *testing.T) {
	ctx := context.Background()
	sharedBlobRoot := kb.SharedBlobRoot(t)
	embedder := newFixtureEmbedder(32)

	writer := kb.NewTestHarness(t, "unused").WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
	t.Cleanup(writer.Cleanup)
	registerFormatOnHarness(t, writer)

	reader := kb.NewTestHarness(t, "unused").WithBlobRoot(sharedBlobRoot).WithEmbedder(embedder).Setup()
	t.Cleanup(reader.Cleanup)
	registerFormatOnHarness(t, reader)

	writerKB := writer.KB()
	readerKB := reader.KB()

	tenantIDs := []string{"tenant-vis-a", "tenant-vis-b"}
	for _, tenantID := range tenantIDs {
		require.NoError(t, writerKB.UpsertDocsAndUpload(ctx, tenantID, []kb.Document{{
			ID:   tenantID + "-seed",
			Text: "seed " + tenantID,
		}}))
		seedVec, err := readerKB.Embed(ctx, "seed "+tenantID)
		require.NoError(t, err)
		_, err = readerKB.Search(ctx, tenantID, seedVec, &kb.SearchOptions{TopK: 1})
		require.NoError(t, err)
	}

	for _, tenantID := range tenantIDs {
		newDoc := kb.Document{ID: tenantID + "-fresh", Text: "fresh write for " + tenantID}
		require.NoError(t, writerKB.UpsertDocsAndUpload(ctx, tenantID, []kb.Document{newDoc}))
		require.NoError(t, waitForDocVisible(ctx, readerKB, tenantID, newDoc.Text, newDoc.ID, 4*time.Second))
	}
}

func readFixtureDocument(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "testdata", "documents", name)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	text := strings.TrimSpace(string(data))
	require.NotEmpty(t, text)
	return text
}

func nonEmptyLines(text string) []string {
	parts := strings.Split(text, "\n")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func buildChunkedDocuments(prefix, text string, wordsPerChunk, overlap int) []kb.Document {
	tokens := tokenizeText(text)
	if wordsPerChunk <= 0 {
		wordsPerChunk = 120
	}
	if overlap < 0 || overlap >= wordsPerChunk {
		overlap = wordsPerChunk / 4
	}
	step := wordsPerChunk - overlap
	if step <= 0 {
		step = wordsPerChunk
	}

	docs := make([]kb.Document, 0)
	for start := 0; start < len(tokens); start += step {
		end := start + wordsPerChunk
		if end > len(tokens) {
			end = len(tokens)
		}
		chunk := strings.TrimSpace(strings.Join(tokens[start:end], " "))
		if chunk == "" {
			continue
		}
		docs = append(docs, kb.Document{
			ID:   fmt.Sprintf("%s-%03d", prefix, len(docs)),
			Text: chunk,
		})
		if end == len(tokens) {
			break
		}
	}
	return docs
}

func tokenizeText(text string) []string {
	fields := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

func waitForDocVisible(ctx context.Context, loader *kb.KB, kbID, text, docID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := queryContainsDocID(ctx, loader, kbID, text, docID, 10)
		if err == nil && ok {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("doc not visible within timeout: kb=%s id=%s", kbID, docID)
}

func queryContainsDocID(ctx context.Context, loader *kb.KB, kbID, text, docID string, k int) (bool, error) {
	qVec, err := loader.Embed(ctx, text)
	if err != nil {
		return false, err
	}
	results, err := loader.Search(ctx, kbID, qVec, &kb.SearchOptions{TopK: k})
	if err != nil {
		return false, err
	}
	for _, r := range results {
		if r.ID == docID {
			return true, nil
		}
	}
	return false, nil
}

func bruteForceTopK(docEmbeddings map[string][]float32, query []float32, k int) []string {
	type scoredID struct {
		id       string
		distance float64
	}
	scored := make([]scoredID, 0, len(docEmbeddings))
	for id, emb := range docEmbeddings {
		scored = append(scored, scoredID{id: id, distance: l2Distance(emb, query)})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].distance == scored[j].distance {
			return scored[i].id < scored[j].id
		}
		return scored[i].distance < scored[j].distance
	})
	if k > len(scored) {
		k = len(scored)
	}
	out := make([]string, 0, k)
	for i := 0; i < k; i++ {
		out = append(out, scored[i].id)
	}
	return out
}

func l2Distance(a, b []float32) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var sum float64
	for i := 0; i < n; i++ {
		d := float64(a[i] - b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

func overlapCount(a, b []string) int {
	set := make(map[string]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	count := 0
	for _, id := range b {
		if _, ok := set[id]; ok {
			count++
		}
	}
	return count
}
