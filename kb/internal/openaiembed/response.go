package openaiembed

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Response struct {
	Data []struct {
		Index     *int      `json:"index"`
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

func Decode(statusCode int, body io.Reader, want int) (Response, error) {
	if statusCode != http.StatusOK {
		data, _ := io.ReadAll(body)
		if len(data) == 0 {
			return Response{}, fmt.Errorf("openai compatible embed request failed with status %d", statusCode)
		}
		return Response{}, fmt.Errorf(
			"openai compatible embed request failed with status %d: %s",
			statusCode,
			strings.TrimSpace(string(data)),
		)
	}
	var parsed Response
	if err := json.NewDecoder(body).Decode(&parsed); err != nil {
		return parsed, fmt.Errorf("decode openai compatible embed response: %w", err)
	}
	if len(parsed.Data) != want {
		return parsed, fmt.Errorf(
			"openai compatible embed response contained %d embeddings for %d inputs",
			len(parsed.Data),
			want,
		)
	}
	return parsed, nil
}

func Vectors(parsed Response, want int) ([][]float32, error) {
	vectors := make([][]float32, want)
	for fallbackIndex, item := range parsed.Data {
		idx := fallbackIndex
		if item.Index != nil && *item.Index >= 0 && *item.Index < want {
			idx = *item.Index
		}
		if len(item.Embedding) == 0 {
			return nil, fmt.Errorf("openai compatible embed response contained empty embedding at index %d", idx)
		}
		vectors[idx] = Float64sToFloat32s(item.Embedding)
	}
	return requireAllVectors(vectors)
}

func Float64sToFloat32s(values []float64) []float32 {
	vector := make([]float32, len(values))
	for i, v := range values {
		vector[i] = float32(v)
	}
	return vector
}

func requireAllVectors(vectors [][]float32) ([][]float32, error) {
	for i, vector := range vectors {
		if len(vector) == 0 {
			return nil, fmt.Errorf("openai compatible embed response missing embedding at index %d", i)
		}
	}
	return vectors, nil
}
