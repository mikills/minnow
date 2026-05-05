package kb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

const (
	defaultOllamaBaseURL = "http://localhost:11434"
	defaultOllamaModel   = "all-minilm"

	defaultGrapherModel   = "gemma3:4b"
	defaultGrapherWorkers = 4
)

const ollamaGrapherPromptTemplate = `You are a knowledge-graph extraction system. Extract named entities and directed relationships from the text below.

Output schema (return exactly this JSON structure, one line, no markdown fences):
{"entities":["string"],"edges":[{"src":"string","dst":"string","rel":"string","weight":1.0}]}

Entity rules:
- Extract people, organizations, locations, products, events, concepts, and technical terms.
- Use the fullest canonical name found in the text (e.g. "John Smith" not "John", "ACME Corporation" not "ACME").
- Resolve pronouns and abbreviations to their full entity name when clearly identifiable.
- No duplicate entity names in the list.

Edge rules:
- src is the subject (actor), dst is the object (target) of the relationship.
- rel is a lowercase snake_case verb phrase (e.g. works_at, founded_by, located_in, acquired, authored).
- Both src and dst must appear in the entities list.
- weight is a confidence/relevance float from 0.1 to 1.0; use 1.0 when clearly stated, lower when implied.
- No duplicate edges.

If no entities can be extracted, return {"entities":[],"edges":[]}.

Example:
Input: "In 2015, Sundar Pichai became CEO of Google. Google is headquartered in Mountain View, California."
Output: {"entities":["Sundar Pichai","Google","Mountain View","California"],"edges":[{"src":"Sundar Pichai","dst":"Google","rel":"ceo_of","weight":1.0},{"src":"Google","dst":"Mountain View","rel":"headquartered_in","weight":1.0},{"src":"Mountain View","dst":"California","rel":"located_in","weight":1.0}]}

Text:
%s`

// OllamaEmbedder implements Embedder using Ollama /api/embed.
type OllamaEmbedder struct {
	BaseURL string
	Model   string
}

// NewOllamaEmbedder creates an Ollama embedder with optional overrides.
func NewOllamaEmbedder(baseURL, model string) *OllamaEmbedder {
	trimmedBaseURL := strings.TrimSpace(baseURL)
	if trimmedBaseURL == "" {
		trimmedBaseURL = defaultOllamaBaseURL
	}

	trimmedModel := strings.TrimSpace(model)
	if trimmedModel == "" {
		trimmedModel = defaultOllamaModel
	}

	return &OllamaEmbedder{
		BaseURL: trimmedBaseURL,
		Model:   trimmedModel,
	}
}

// Embed requests a vector embedding from Ollama.
func (o *OllamaEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	requestBody, err := json.Marshal(map[string]string{
		"model": o.Model,
		"input": input,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal ollama embed request: %w", err)
	}

	endpoint := strings.TrimRight(o.BaseURL, "/") + "/api/embed"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("create ollama embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	reply, err := closeableHTTPDo(http.DefaultClient, req)
	if err != nil {
		return nil, fmt.Errorf("request embeddings from ollama: %w", err)
	}
	defer reply.Close()

	if reply.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(reply.Body)
		if len(body) == 0 {
			return nil, fmt.Errorf("ollama embed request failed with status %d", reply.StatusCode)
		}
		return nil, fmt.Errorf("ollama embed request failed with status %d: %s", reply.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		Embeddings [][]float64 `json:"embeddings"`
	}
	if err := json.NewDecoder(reply.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode ollama embed response: %w", err)
	}

	if len(parsed.Embeddings) == 0 || len(parsed.Embeddings[0]) == 0 {
		return nil, fmt.Errorf("ollama embed response contained empty embeddings")
	}

	vector := make([]float32, len(parsed.Embeddings[0]))
	for i, v := range parsed.Embeddings[0] {
		vector[i] = float32(v)
	}

	return vector, nil
}

// Ping validates Ollama connectivity by requesting a short embedding.
func (o *OllamaEmbedder) Ping(ctx context.Context) error {
	_, err := o.Embed(ctx, "ping")
	return err
}

// OllamaGrapher extracts graph candidates using Ollama /api/generate.
type OllamaGrapher struct {
	BaseURL     string
	Model       string
	MaxParallel int
}

// NewOllamaGrapher creates an Ollama grapher with optional overrides.
func NewOllamaGrapher(baseURL, model string) *OllamaGrapher {
	trimmedBaseURL := strings.TrimSpace(baseURL)
	if trimmedBaseURL == "" {
		trimmedBaseURL = defaultOllamaBaseURL
	}

	trimmedModel := strings.TrimSpace(model)
	if trimmedModel == "" {
		trimmedModel = defaultGrapherModel
	}

	return &OllamaGrapher{
		BaseURL:     trimmedBaseURL,
		Model:       trimmedModel,
		MaxParallel: defaultGrapherWorkers,
	}
}

type ollamaGenerateRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Format string `json:"format"`
	Stream bool   `json:"stream"`
}

type ollamaGenerateResponse struct {
	Response string `json:"response"`
}

type ollamaGrapherResult struct {
	Entities []string            `json:"entities"`
	Edges    []ollamaGrapherEdge `json:"edges"`
}

type chunkResult struct {
	entities []EntityCandidate
	edges    []EdgeCandidate
	err      error
}

type ollamaGrapherEdge struct {
	Src    string  `json:"src"`
	Dst    string  `json:"dst"`
	Rel    string  `json:"rel"`
	Weight float64 `json:"weight"`
}

var _ Grapher = (*OllamaGrapher)(nil)

func (o *OllamaGrapher) extractChunk(ctx context.Context, chunk Chunk) ([]EntityCandidate, []EdgeCandidate, error) {
	prompt := fmt.Sprintf(ollamaGrapherPromptTemplate, chunk.Text)
	payload, err := json.Marshal(ollamaGenerateRequest{
		Model:  o.Model,
		Prompt: prompt,
		Format: "json",
		Stream: false,
	})
	if err != nil {
		return nil, nil, err
	}

	url := strings.TrimRight(o.BaseURL, "/") + "/api/generate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	reply, err := closeableHTTPDo(http.DefaultClient, req)
	if err != nil {
		return nil, nil, err
	}
	defer reply.Close()

	if reply.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(reply.Body, 512))
		if readErr != nil {
			return nil, nil, fmt.Errorf("ollama grapher request failed with status %d", reply.StatusCode)
		}
		return nil, nil, fmt.Errorf("ollama grapher request failed with status %d: %s", reply.StatusCode, string(body))
	}

	var response ollamaGenerateResponse
	if err := json.NewDecoder(reply.Body).Decode(&response); err != nil {
		return nil, nil, err
	}

	var result ollamaGrapherResult
	if err := json.Unmarshal([]byte(response.Response), &result); err != nil {
		return nil, nil, fmt.Errorf("ollama grapher parse failed for chunk %s: %w", chunk.ChunkID, err)
	}

	return ollamaGraphCandidates(result, chunk.ChunkID), ollamaEdgeCandidates(result, chunk.ChunkID), nil
}

func ollamaGraphCandidates(result ollamaGrapherResult, chunkID string) []EntityCandidate {
	seen := make(map[string]struct{}, len(result.Entities))
	entities := make([]EntityCandidate, 0, len(result.Entities))
	for _, entityName := range result.Entities {
		trimmed := strings.TrimSpace(entityName)
		if trimmed == "" || hasSeenEntity(seen, trimmed) {
			continue
		}
		seen[trimmed] = struct{}{}
		entities = append(entities, EntityCandidate{Name: trimmed, ChunkID: chunkID})
	}
	return entities
}

func hasSeenEntity(seen map[string]struct{}, name string) bool {
	_, exists := seen[name]
	return exists
}

func ollamaEdgeCandidates(result ollamaGrapherResult, chunkID string) []EdgeCandidate {
	edges := make([]EdgeCandidate, 0, len(result.Edges))
	for _, edge := range result.Edges {
		edges = append(edges, EdgeCandidate{Src: edge.Src, Dst: edge.Dst, RelType: edge.Rel, Weight: positiveEdgeWeight(edge.Weight), ChunkID: chunkID})
	}
	return edges
}

func positiveEdgeWeight(weight float64) float64 {
	if weight <= 0 {
		return 1.0
	}
	return weight
}

// Extract implements Grapher.
func (o *OllamaGrapher) Extract(ctx context.Context, chunks []Chunk) (*GraphExtraction, error) {
	if len(chunks) == 0 {
		return &GraphExtraction{}, nil
	}

	workers := grapherWorkerCount(o.MaxParallel, len(chunks))
	if workers <= 1 {
		return o.extractChunksSerial(ctx, chunks)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]chunkResult, len(chunks))
	jobs := make(chan int, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		startOllamaGrapherWorker(ollamaWorkerState{ctx: ctx, grapher: o, chunks: chunks, jobs: jobs, results: results, cancel: cancel, wg: &wg})
	}

	// Producer goroutine: sends chunk indices to the jobs channel.
	// Context cancellation is propagated to workers via extractChunk(ctx, ...),
	// which checks ctx on each HTTP call. The producer stops sending on ctx.Done().
	go func() {
	outer:
		for i := range chunks {
			select {
			case <-ctx.Done():
				break outer
			case jobs <- i:
			}
		}
		close(jobs)
	}()

	wg.Wait()

	return graphExtractionFromChunkResults(results)
}

func grapherWorkerCount(maxParallel int, chunks int) int {
	workers := maxParallel
	if workers <= 0 {
		workers = defaultGrapherWorkers
	}
	if workers > chunks {
		return chunks
	}
	return workers
}

func (o *OllamaGrapher) extractChunksSerial(ctx context.Context, chunks []Chunk) (*GraphExtraction, error) {
	allEntities := make([]EntityCandidate, 0)
	allEdges := make([]EdgeCandidate, 0)
	for _, chunk := range chunks {
		entities, edges, err := o.extractChunk(ctx, chunk)
		if err != nil {
			return nil, err
		}
		allEntities = append(allEntities, entities...)
		allEdges = append(allEdges, edges...)
	}
	return &GraphExtraction{Entities: allEntities, Edges: allEdges}, nil
}

func graphExtractionFromChunkResults(results []chunkResult) (*GraphExtraction, error) {
	allEntities := make([]EntityCandidate, 0)
	allEdges := make([]EdgeCandidate, 0)
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		allEntities = append(allEntities, results[i].entities...)
		allEdges = append(allEdges, results[i].edges...)
	}
	return &GraphExtraction{Entities: allEntities, Edges: allEdges}, nil
}

type ollamaWorkerState struct {
	ctx     context.Context
	grapher *OllamaGrapher
	chunks  []Chunk
	jobs    <-chan int
	results []chunkResult
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
}

func startOllamaGrapherWorker(state ollamaWorkerState) {
	state.wg.Add(1)
	go runOllamaGrapherWorker(state)
}

func runOllamaGrapherWorker(state ollamaWorkerState) {
	defer state.wg.Done()
	for idx := range state.jobs {
		entities, edges, err := state.grapher.extractChunk(state.ctx, state.chunks[idx])
		if err != nil {
			state.results[idx].err = err
			state.cancel()
			continue
		}
		state.results[idx].entities = entities
		state.results[idx].edges = edges
	}
}
