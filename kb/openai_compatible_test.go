package kb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAICompatibleEmbedder(t *testing.T) {
	t.Run("sends_openai_compatible_request", func(t *testing.T) {
		var gotPath string
		var gotAuth string
		var gotBody struct {
			Model      string `json:"model"`
			Input      string `json:"input"`
			Dimensions int    `json:"dimensions"`
		}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotAuth = r.Header.Get("Authorization")
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))
			require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"embedding":[0.25,0.5,0.75]}]}`))
		}))
		defer server.Close()

		embedder, err := NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{
			BaseURL:    server.URL + "/v1/",
			Model:      "text-embedding-3-small",
			Token:      "secret-token",
			Dimensions: 3,
		})
		require.NoError(t, err)

		vec, err := embedder.Embed(context.Background(), "hello")
		require.NoError(t, err)
		require.Equal(t, []float32{0.25, 0.5, 0.75}, vec)
		require.Equal(t, "/v1/embeddings", gotPath)
		require.Equal(t, "Bearer secret-token", gotAuth)
		require.Equal(t, "text-embedding-3-small", gotBody.Model)
		require.Equal(t, "hello", gotBody.Input)
		require.Equal(t, 3, gotBody.Dimensions)
	})

	t.Run("omits_optional_token_and_dimensions", func(t *testing.T) {
		var gotAuth string
		var gotBody map[string]any
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuth = r.Header.Get("Authorization")
			require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
			_, _ = w.Write([]byte(`{"data":[{"embedding":[1,2]}]}`))
		}))
		defer server.Close()

		embedder, err := NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "nomic-embed-text"})
		require.NoError(t, err)
		_, err = embedder.Embed(context.Background(), "hello")
		require.NoError(t, err)
		require.Empty(t, gotAuth)
		require.NotContains(t, gotBody, "dimensions")
	})

	t.Run("rejects_empty_embeddings", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"data":[{"embedding":[]}]}`))
		}))
		defer server.Close()

		embedder, err := NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"})
		require.NoError(t, err)
		_, err = embedder.Embed(context.Background(), "hello")
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty embeddings")
	})

	t.Run("surfaces_non_200_body", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "bad token", http.StatusUnauthorized)
		}))
		defer server.Close()

		embedder, err := NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{BaseURL: server.URL, Model: "model"})
		require.NoError(t, err)
		_, err = embedder.Embed(context.Background(), "hello")
		require.Error(t, err)
		require.Contains(t, err.Error(), "401")
		require.Contains(t, err.Error(), "bad token")
	})

	t.Run("validates_constructor_inputs", func(t *testing.T) {
		_, err := NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{BaseURL: "ftp://example.com", Model: "model"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "scheme")

		_, err = NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{BaseURL: "http://example.com", Model: ""})
		require.Error(t, err)
		require.Contains(t, err.Error(), "model")

		_, err = NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{BaseURL: "http://example.com", Model: "model", Dimensions: -1})
		require.ErrorIs(t, err, ErrInvalidEmbeddingDimension)
	})

	t.Run("constructor sets default timeout", func(t *testing.T) {
		emb, err := NewOpenAICompatibleEmbedder(OpenAICompatibleEmbedderConfig{BaseURL: "http://example.com", Model: "model"})
		require.NoError(t, err)
		require.NotNil(t, emb.HTTPClient)
		require.Equal(t, defaultOpenAICompatibleTimeout, emb.HTTPClient.Timeout)
	})
}
