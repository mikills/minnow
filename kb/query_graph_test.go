package kb

import (
	"context"
	"errors"
	"testing"
)

func TestSearch(t *testing.T) {
	calledErr := errors.New("backend should not be called")
	mock := &mockArtifactFormat{
		queryRagFn: func(context.Context, RagQueryRequest) ([]ExpandedResult, error) {
			return nil, calledErr
		},
		queryGraphFn: func(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
			return nil, calledErr
		},
	}

	kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(), WithArtifactFormat(mock))

	t.Run("vector_mode_empty_query_vec", func(t *testing.T) {
		_, err := kb.Search(context.Background(), "kb", nil, &SearchOptions{TopK: 1})
		if !errors.Is(err, ErrInvalidQueryRequest) {
			t.Fatalf("expected ErrInvalidQueryRequest, got %v", err)
		}
	})

	t.Run("graph_mode_invalid_max_distance", func(t *testing.T) {
		zero := 0.0
		_, err := kb.Search(context.Background(), "kb", []float32{0.1}, &SearchOptions{Mode: SearchModeGraph, TopK: 1, MaxDistance: &zero})
		if !errors.Is(err, ErrInvalidQueryRequest) {
			t.Fatalf("expected ErrInvalidQueryRequest, got %v", err)
		}
	})
}
