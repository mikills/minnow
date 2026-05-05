package kb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultOpenAICompatibleBaseURL = "https://api.openai.com/v1"
	// defaultOpenAICompatibleTimeout caps a single embedding request when the
	// caller does not pass its own deadline. Workers that always plumb a
	// context deadline get the smaller of the two; callers without one get a
	// finite ceiling instead of hanging forever.
	defaultOpenAICompatibleTimeout = 30 * time.Second
)

// OpenAICompatibleEmbedder requests embeddings from OpenAI-compatible
// /v1/embeddings APIs, including hosted OpenAI-compatible providers and
// Ollama's compatibility endpoint.
type OpenAICompatibleEmbedder struct {
	BaseURL    string
	Model      string
	Token      string
	Dimensions int
	HTTPClient *http.Client
}

type OpenAICompatibleEmbedderConfig struct {
	BaseURL    string
	Model      string
	Token      string
	Dimensions int
}

// NewOpenAICompatibleEmbedder creates an embedder for OpenAI-compatible APIs.
func NewOpenAICompatibleEmbedder(cfg OpenAICompatibleEmbedderConfig) (*OpenAICompatibleEmbedder, error) {
	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = defaultOpenAICompatibleBaseURL
	}
	model := strings.TrimSpace(cfg.Model)
	if model == "" {
		return nil, fmt.Errorf("openai compatible embedder model cannot be empty")
	}
	if cfg.Dimensions < 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidEmbeddingDimension, cfg.Dimensions)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("openai compatible embedder base_url must be a valid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("openai compatible embedder base_url scheme must be http or https (got %q)", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("openai compatible embedder base_url must include a host")
	}

	return &OpenAICompatibleEmbedder{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		Model:      model,
		Token:      strings.TrimSpace(cfg.Token),
		Dimensions: cfg.Dimensions,
		HTTPClient: &http.Client{Timeout: defaultOpenAICompatibleTimeout},
	}, nil
}

// Embed requests a single embedding for input.
func (e *OpenAICompatibleEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	vectors, err := e.EmbedBatch(ctx, []string{input})
	if err != nil {
		return nil, err
	}
	return vectors[0], nil
}

// EmbedBatch requests embeddings for multiple inputs in one provider call.
type openAIEmbeddingResponse struct {
	Data []struct {
		Index     *int      `json:"index"`
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

func (e *OpenAICompatibleEmbedder) EmbedBatch(ctx context.Context, inputs []string) ([][]float32, error) {
	cleanInputs, err := e.validateBatchInputs(inputs)
	if err != nil {
		return nil, err
	}
	reply, err := e.doEmbeddingRequest(ctx, cleanInputs)
	if err != nil {
		return nil, err
	}
	defer reply.Close()
	parsed, err := decodeOpenAIEmbeddingResponse(reply, len(cleanInputs))
	if err != nil {
		return nil, err
	}
	return openAIEmbeddingVectors(parsed, len(cleanInputs))
}

func (e *OpenAICompatibleEmbedder) validateBatchInputs(inputs []string) ([]string, error) {
	if e == nil {
		return nil, fmt.Errorf("openai compatible embedder is nil")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("inputs cannot be empty")
	}
	if strings.TrimSpace(e.Model) == "" {
		return nil, fmt.Errorf("openai compatible embedder model cannot be empty")
	}
	if e.Dimensions < 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidEmbeddingDimension, e.Dimensions)
	}
	cleanInputs := make([]string, len(inputs))
	for i, input := range inputs {
		if strings.TrimSpace(input) == "" {
			return nil, fmt.Errorf("input cannot be empty")
		}
		cleanInputs[i] = input
	}
	return cleanInputs, nil
}

func (e *OpenAICompatibleEmbedder) doEmbeddingRequest(ctx context.Context, inputs []string) (*closeableHTTPResponse, error) {
	requestBody, err := json.Marshal(e.embeddingRequestBody(inputs))
	if err != nil {
		return nil, fmt.Errorf("marshal openai compatible embed request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(e.BaseURL, "/")+"/embeddings", bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("create openai compatible embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(e.Token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	reply, err := closeableHTTPDo(e.httpClient(), req)
	if err != nil {
		return nil, fmt.Errorf("request embeddings from openai compatible API: %w", err)
	}
	return reply, nil
}

func (e *OpenAICompatibleEmbedder) embeddingRequestBody(inputs []string) map[string]any {
	body := map[string]any{"model": e.Model, "input": inputs}
	if e.Dimensions > 0 {
		body["dimensions"] = e.Dimensions
	}
	return body
}

func (e *OpenAICompatibleEmbedder) httpClient() *http.Client {
	if e.HTTPClient != nil {
		return e.HTTPClient
	}
	return &http.Client{Timeout: defaultOpenAICompatibleTimeout}
}

func decodeOpenAIEmbeddingResponse(reply *closeableHTTPResponse, want int) (openAIEmbeddingResponse, error) {
	if reply.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(reply.Body)
		if len(body) == 0 {
			return openAIEmbeddingResponse{}, fmt.Errorf("openai compatible embed request failed with status %d", reply.StatusCode)
		}
		return openAIEmbeddingResponse{}, fmt.Errorf("openai compatible embed request failed with status %d: %s", reply.StatusCode, strings.TrimSpace(string(body)))
	}
	var parsed openAIEmbeddingResponse
	if err := json.NewDecoder(reply.Body).Decode(&parsed); err != nil {
		return parsed, fmt.Errorf("decode openai compatible embed response: %w", err)
	}
	if len(parsed.Data) != want {
		return parsed, fmt.Errorf("openai compatible embed response contained %d embeddings for %d inputs", len(parsed.Data), want)
	}
	return parsed, nil
}

func openAIEmbeddingVectors(parsed openAIEmbeddingResponse, want int) ([][]float32, error) {
	vectors := make([][]float32, want)
	for fallbackIndex, item := range parsed.Data {
		idx := fallbackIndex
		if item.Index != nil && *item.Index >= 0 && *item.Index < want {
			idx = *item.Index
		}
		if len(item.Embedding) == 0 {
			return nil, fmt.Errorf("openai compatible embed response contained empty embedding at index %d", idx)
		}
		vectors[idx] = float64sToFloat32s(item.Embedding)
	}
	return requireAllOpenAIVectors(vectors)
}

func float64sToFloat32s(values []float64) []float32 {
	vector := make([]float32, len(values))
	for i, v := range values {
		vector[i] = float32(v)
	}
	return vector
}

func requireAllOpenAIVectors(vectors [][]float32) ([][]float32, error) {
	for i, vector := range vectors {
		if len(vector) == 0 {
			return nil, fmt.Errorf("openai compatible embed response missing embedding at index %d", i)
		}
	}
	return vectors, nil
}
