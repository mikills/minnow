package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/mikills/minnow/kb/graphbuild"
)

const ollamaGrapherPromptTemplate = `You are a knowledge-graph extraction system. Extract named entities and directed relationships from the text below.

Output schema (return exactly this JSON structure, one line, no markdown fences):
{"entities":["string"],"edges":[{"src":"string","dst":"string","rel":"string","weight":1.0}]}

Entity rules:
- Extract people, organizations, locations, products, events, concepts, and technical terms.
- Use the fullest canonical name found in the text.
- Resolve pronouns and abbreviations to their full entity name when clearly identifiable.
- No duplicate entity names in the list.

Edge rules:
- src is the subject, dst is the object of the relationship.
- rel is a lowercase snake_case verb phrase.
- Both src and dst must appear in the entities list.
- weight is a confidence/relevance float from 0.1 to 1.0.
- No duplicate edges.

If no entities can be extracted, return {"entities":[],"edges":[]}.

Text:
%s`

const (
	defaultOllamaBaseURL  = DefaultBaseURL
	defaultGrapherModel   = "gemma3:4b"
	defaultGrapherWorkers = 1 << 2
)

// Grapher extracts graph candidates using Ollama /api/generate.
type Grapher struct {
	BaseURL     string
	Model       string
	MaxParallel int
}

// NewGrapher creates an Ollama grapher with optional overrides.
func NewGrapher(baseURL, model string) *Grapher {
	trimmedBaseURL := strings.TrimSpace(baseURL)
	if trimmedBaseURL == "" {
		trimmedBaseURL = defaultOllamaBaseURL
	}

	trimmedModel := strings.TrimSpace(model)
	if trimmedModel == "" {
		trimmedModel = defaultGrapherModel
	}

	return &Grapher{
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
	entities []graphbuild.EntityCandidate
	edges    []graphbuild.EdgeCandidate
	err      error
}

type ollamaGrapherEdge struct {
	Src    string  `json:"src"`
	Dst    string  `json:"dst"`
	Rel    string  `json:"rel"`
	Weight float64 `json:"weight"`
}

var _ graphbuild.Grapher = (*Grapher)(nil)

func (o *Grapher) extractChunk(
	ctx context.Context,
	chunk graphbuild.Chunk,
) ([]graphbuild.EntityCandidate, []graphbuild.EdgeCandidate, error) {
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

func ollamaGraphCandidates(result ollamaGrapherResult, chunkID string) []graphbuild.EntityCandidate {
	seen := make(map[string]struct{}, len(result.Entities))
	entities := make([]graphbuild.EntityCandidate, 0, len(result.Entities))
	for _, entityName := range result.Entities {
		trimmed := strings.TrimSpace(entityName)
		if trimmed == "" || hasSeenEntity(seen, trimmed) {
			continue
		}
		seen[trimmed] = struct{}{}
		entities = append(entities, graphbuild.EntityCandidate{Name: trimmed, ChunkID: chunkID})
	}
	return entities
}

func hasSeenEntity(seen map[string]struct{}, name string) bool {
	_, exists := seen[name]
	return exists
}

func ollamaEdgeCandidates(result ollamaGrapherResult, chunkID string) []graphbuild.EdgeCandidate {
	edges := make([]graphbuild.EdgeCandidate, 0, len(result.Edges))
	for _, edge := range result.Edges {
		edges = append(
			edges,
			graphbuild.EdgeCandidate{
				Src:     edge.Src,
				Dst:     edge.Dst,
				RelType: edge.Rel,
				Weight:  positiveEdgeWeight(edge.Weight),
				ChunkID: chunkID,
			},
		)
	}
	return edges
}

func positiveEdgeWeight(weight float64) float64 {
	if weight <= 0 {
		return float64(1)
	}
	return weight
}

// Extract implements Grapher.
func (o *Grapher) Extract(ctx context.Context, chunks []graphbuild.Chunk) (*graphbuild.Extraction, error) {
	if len(chunks) == 0 {
		return &graphbuild.Extraction{}, nil
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
		startGrapherWorker(
			ollamaWorkerState{
				ctx:     ctx,
				grapher: o,
				chunks:  chunks,
				jobs:    jobs,
				results: results,
				cancel:  cancel,
				wg:      &wg,
			},
		)
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

func (o *Grapher) extractChunksSerial(ctx context.Context, chunks []graphbuild.Chunk) (*graphbuild.Extraction, error) {
	allEntities := make([]graphbuild.EntityCandidate, 0)
	allEdges := make([]graphbuild.EdgeCandidate, 0)
	for _, chunk := range chunks {
		entities, edges, err := o.extractChunk(ctx, chunk)
		if err != nil {
			return nil, err
		}
		allEntities = append(allEntities, entities...)
		allEdges = append(allEdges, edges...)
	}
	return &graphbuild.Extraction{Entities: allEntities, Edges: allEdges}, nil
}

func graphExtractionFromChunkResults(results []chunkResult) (*graphbuild.Extraction, error) {
	allEntities := make([]graphbuild.EntityCandidate, 0)
	allEdges := make([]graphbuild.EdgeCandidate, 0)
	for i := range results {
		if results[i].err != nil {
			return nil, results[i].err
		}
		allEntities = append(allEntities, results[i].entities...)
		allEdges = append(allEdges, results[i].edges...)
	}
	return &graphbuild.Extraction{Entities: allEntities, Edges: allEdges}, nil
}

type ollamaWorkerState struct {
	ctx     context.Context
	grapher *Grapher
	chunks  []graphbuild.Chunk
	jobs    <-chan int
	results []chunkResult
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
}

func startGrapherWorker(state ollamaWorkerState) {
	state.wg.Add(1)
	go runGrapherWorker(state)
}

func runGrapherWorker(state ollamaWorkerState) {
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
