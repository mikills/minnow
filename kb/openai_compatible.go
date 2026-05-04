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
func (e *OpenAICompatibleEmbedder) EmbedBatch(ctx context.Context, inputs []string) ([][]float32, error) {
	if e == nil {
		return nil, fmt.Errorf("openai compatible embedder is nil")
	}
	if len(inputs) == 0 {
		return nil, fmt.Errorf("inputs cannot be empty")
	}
	cleanInputs := make([]string, len(inputs))
	for i, input := range inputs {
		if strings.TrimSpace(input) == "" {
			return nil, fmt.Errorf("input cannot be empty")
		}
		cleanInputs[i] = input
	}
	if strings.TrimSpace(e.Model) == "" {
		return nil, fmt.Errorf("openai compatible embedder model cannot be empty")
	}
	if e.Dimensions < 0 {
		return nil, fmt.Errorf("%w: got %d", ErrInvalidEmbeddingDimension, e.Dimensions)
	}

	body := map[string]any{
		"model": e.Model,
		"input": cleanInputs,
	}
	if e.Dimensions > 0 {
		body["dimensions"] = e.Dimensions
	}
	requestBody, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal openai compatible embed request: %w", err)
	}

	endpoint := strings.TrimRight(e.BaseURL, "/") + "/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("create openai compatible embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(e.Token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := e.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultOpenAICompatibleTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request embeddings from openai compatible API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		if len(body) == 0 {
			return nil, fmt.Errorf("openai compatible embed request failed with status %d", resp.StatusCode)
		}
		return nil, fmt.Errorf("openai compatible embed request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		Data []struct {
			Index     *int      `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("decode openai compatible embed response: %w", err)
	}
	if len(parsed.Data) != len(cleanInputs) {
		return nil, fmt.Errorf("openai compatible embed response contained %d embeddings for %d inputs", len(parsed.Data), len(cleanInputs))
	}
	vectors := make([][]float32, len(cleanInputs))
	for fallbackIndex, item := range parsed.Data {
		idx := fallbackIndex
		if item.Index != nil && *item.Index >= 0 && *item.Index < len(cleanInputs) {
			idx = *item.Index
		}
		if len(item.Embedding) == 0 {
			return nil, fmt.Errorf("openai compatible embed response contained empty embedding at index %d", idx)
		}
		vector := make([]float32, len(item.Embedding))
		for i, v := range item.Embedding {
			vector[i] = float32(v)
		}
		vectors[idx] = vector
	}
	for i, vector := range vectors {
		if len(vector) == 0 {
			return nil, fmt.Errorf("openai compatible embed response missing embedding at index %d", i)
		}
	}
	return vectors, nil
}
