package openaiembedder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mikills/minnow/kb/internal/openaiembed"
)

var ErrInvalidEmbeddingDimension = errors.New("invalid embedding dimension")

type closeableHTTPResponse struct{ *http.Response }

func closeableHTTPDo(client *http.Client, req *http.Request) (*closeableHTTPResponse, error) {
	return newCloseableHTTPResponse(client.Do(req))
}

func newCloseableHTTPResponse(resp *http.Response, err error) (*closeableHTTPResponse, error) {
	if err != nil {
		return nil, err
	}
	return &closeableHTTPResponse{Response: resp}, nil
}

func (r *closeableHTTPResponse) Close() error {
	if r == nil || r.Body == nil {
		return nil
	}
	return r.Body.Close()
}

type closeableResponse struct{ *http.Response }

func (r *closeableResponse) Close() error {
	if r == nil || r.Body == nil {
		return nil
	}
	return r.Body.Close()
}

const (
	defaultOpenAICompatibleBaseURL = "https://api.openai.com/v1"
	// defaultOpenAICompatibleTimeout caps a single embedding request when the
	// caller does not pass its own deadline. Workers that always plumb a
	// context deadline get the smaller of the two. callers without one get a
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
		return nil, fmt.Errorf(
			"openai compatible embedder base_url scheme must be http or https (got %q)",
			parsed.Scheme,
		)
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
func (e *OpenAICompatibleEmbedder) EmbedBatch(ctx context.Context, inputs []string) (out [][]float32, err error) {
	cleanInputs, err := e.validateBatchInputs(inputs)
	if err != nil {
		return nil, err
	}
	requestBody, err := json.Marshal(e.embeddingRequestBody(cleanInputs))
	if err != nil {
		return nil, fmt.Errorf("marshal openai compatible embed request: %w", err)
	}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		strings.TrimRight(e.BaseURL, "/")+"/embeddings",
		bytes.NewReader(requestBody),
	)
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
	defer func() {
		if closeErr := reply.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	parsed, err := openaiembed.Decode(reply.StatusCode, reply.Body, len(cleanInputs))
	if err != nil {
		return nil, err
	}
	return openaiembed.Vectors(parsed, len(cleanInputs))
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
