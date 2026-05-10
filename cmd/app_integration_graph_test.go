package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type queryResultWithScoresPayload struct {
	Results []struct {
		ID         string   `json:"id"`
		Content    string   `json:"content"`
		Distance   float64  `json:"distance"`
		Score      *float64 `json:"score"`
		GraphScore *float64 `json:"graph_score"`
	} `json:"results"`
}

func testAppQueryFieldsGraph(t *testing.T) {
	kbID := "kb-app-query-graph-fields"
	baseURL := setupAppTestWithOptions(
		t,
		appKeywordEmbedder{},
		kbID,
		kb.WithGraphBuilder(&kb.GraphBuilder{
			Chunker:       appTestChunker{},
			Grapher:       appTestGrapher{},
			Canonicalizer: appIdentityCanonicalizer{},
		}),
	)

	ingestResp, err := postJSON(baseURL+"/rag/ingest", map[string]any{
		kbIDContextKey:  kbID,
		"graph_enabled": true,
		"documents": []map[string]string{
			{"id": "doc-finance", "text": "finance revenue services growth"},
			{"id": "doc-food", "text": "pasta carbonara parmesan recipe"},
		},
		"chunk_size": 300,
	})
	require.NoError(t, err)
	defer ingestResp.Body.Close()
	require.Equal(t, http.StatusAccepted, ingestResp.StatusCode)
	var ingestPayload ingestAcceptedPayload
	require.NoError(t, json.NewDecoder(ingestResp.Body).Decode(&ingestPayload))
	_, waitErr := waitForOperation(baseURL, ingestPayload.EventID)
	require.NoError(t, waitErr)

	testCases := []struct {
		name       string
		searchMode string
		wantScores bool
	}{
		{name: "vector", searchMode: "vector", wantScores: false},
		{name: "graph", searchMode: "graph", wantScores: true},
		{name: "graph_upper", searchMode: "GRAPH", wantScores: true},
		{name: "adaptive_mixed_case", searchMode: "AdApTiVe", wantScores: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := postJSON(baseURL+"/rag/query", map[string]any{
				kbIDContextKey: kbID,
				"query":        "finance services growth",
				"k":            5,
				"search_mode":  tc.searchMode,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var payload queryResultWithScoresPayload
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
			require.NotEmpty(t, payload.Results)
			assert.NotEmpty(t, payload.Results[0].ID)
			assert.NotEmpty(t, payload.Results[0].Content)

			if tc.wantScores {
				assert.NotNil(t, payload.Results[0].Score)
				assert.NotNil(t, payload.Results[0].GraphScore)
			} else {
				assert.Nil(t, payload.Results[0].Score)
				assert.Nil(t, payload.Results[0].GraphScore)
			}
		})
	}
}

type appTestChunker struct{}

func (appTestChunker) Chunk(_ context.Context, docID string, text string) ([]kb.Chunk, error) {
	return []kb.Chunk{{
		DocID:   docID,
		ChunkID: docID + "-chunk-000",
		Text:    text,
		Start:   0,
		End:     len(text),
	}}, nil
}

type appTestGrapher struct{}

func (appTestGrapher) Extract(_ context.Context, chunks []kb.Chunk) (*kb.GraphExtraction, error) {
	entities := make([]kb.EntityCandidate, 0, len(chunks)*2)
	edges := make([]kb.EdgeCandidate, 0, len(chunks))
	for _, chunk := range chunks {
		entityName := "ent:" + chunk.DocID
		entities = append(entities,
			kb.EntityCandidate{Name: entityName, ChunkID: chunk.ChunkID},
			kb.EntityCandidate{Name: "shared", ChunkID: chunk.ChunkID},
		)
		edges = append(edges, kb.EdgeCandidate{
			Src:     entityName,
			Dst:     "shared",
			RelType: "mentions",
			Weight:  1.0,
			ChunkID: chunk.ChunkID,
		})
	}
	return &kb.GraphExtraction{Entities: entities, Edges: edges}, nil
}

type appIdentityCanonicalizer struct{}

func (appIdentityCanonicalizer) Canonicalize(_ context.Context, name string) (string, error) {
	return name, nil
}

type appKeywordEmbedder struct{}

func (appKeywordEmbedder) Embed(_ context.Context, input string) ([]float32, error) {
	lower := strings.ToLower(input)
	finance := keywordScore(lower, []string{"finance", "revenue", "iphone", "services", "apple", "sales", "margin"})
	food := keywordScore(lower, []string{"pasta", "carbonara", "egg", "parmesan", "recipe", "cook"})
	bias := float32(len(strings.Fields(lower))%5) * 0.01
	return []float32{finance + bias, food + bias, 0.1 + bias}, nil
}

func keywordScore(input string, words []string) float32 {
	score := float32(0)
	for _, w := range words {
		if strings.Contains(input, w) {
			score += 1
		}
	}
	return score
}

type appOllamaEmbedder struct {
	BaseURL string
	Model   string
}

func (o *appOllamaEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	reqBody, err := json.Marshal(map[string]string{
		"model": o.Model,
		"input": input,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(o.BaseURL, "/")+"/api/embed",
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Embeddings) == 0 || len(result.Embeddings[0]) == 0 {
		return nil, fmt.Errorf("empty embeddings in response")
	}

	vec := make([]float32, len(result.Embeddings[0]))
	for i, v := range result.Embeddings[0] {
		vec[i] = float32(v)
	}

	return vec, nil
}

func postJSON(url string, body any) (*http.Response, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func waitForOperation(baseURL, eventID string) (map[string]any, error) {
	deadline := time.Now().Add(10 * time.Second)
	// Start at 200ms so we stay well under the operations endpoint rate
	// limiter (10 req/sec/IP, burst 20) even across multiple sequential
	// polls from tests running on the same host.
	delay := 200 * time.Millisecond
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/rag/operations/" + eventID)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()
			time.Sleep(500 * time.Millisecond)
			continue
		}
		var payload map[string]any
		decodeErr := json.NewDecoder(resp.Body).Decode(&payload)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, decodeErr
		}
		terminal, _ := payload["terminal"].(map[string]any)
		if term, ok := terminal["kind"].(string); ok &&
			(term == string(kb.EventKBPublished) || term == string(kb.EventMediaUploaded)) {
			return payload, nil
		}
		if status, _ := payload["status"].(string); status == string(kb.EventStatusDead) {
			return nil, fmt.Errorf("operation %s failed", eventID)
		}
		if term, ok := terminal["kind"].(string); ok && term == string(kb.EventWorkerFailed) {
			return nil, fmt.Errorf("operation %s failed", eventID)
		}
		time.Sleep(delay)
		if delay < 800*time.Millisecond {
			delay *= 2
		}
	}
	return nil, fmt.Errorf("timeout waiting for operation %s", eventID)
}

func postMultipart(url string, write func(*multipart.Writer) error) (*http.Response, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if err := write(w); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	return http.DefaultClient.Do(req)
}
