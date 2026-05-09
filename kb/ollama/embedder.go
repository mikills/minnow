package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	DefaultBaseURL = "http://localhost:11434"
	DefaultModel   = "all-minilm"
)

type Embedder struct {
	BaseURL string
	Model   string
}

func NewEmbedder(baseURL, model string) *Embedder {
	trimmedBaseURL := strings.TrimSpace(baseURL)
	if trimmedBaseURL == "" {
		trimmedBaseURL = DefaultBaseURL
	}
	trimmedModel := strings.TrimSpace(model)
	if trimmedModel == "" {
		trimmedModel = DefaultModel
	}
	return &Embedder{BaseURL: trimmedBaseURL, Model: trimmedModel}
}

func (o *Embedder) Embed(ctx context.Context, input string) ([]float32, error) {
	requestBody, err := json.Marshal(map[string]string{"model": o.Model, "input": input})
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
		return nil, fmt.Errorf(
			"ollama embed request failed with status %d: %s",
			reply.StatusCode,
			strings.TrimSpace(string(body)),
		)
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

func (o *Embedder) Ping(ctx context.Context) error {
	_, err := o.Embed(ctx, "ping")
	return err
}

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
