package kb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type staticChunker struct{}

func (staticChunker) Chunk(_ context.Context, docID string, text string) ([]Chunk, error) {
	return []Chunk{{DocID: docID, ChunkID: docID + "-chunk-000", Text: text, Start: 0, End: len(text)}}, nil
}

type staticGrapher struct{}

func (staticGrapher) Extract(_ context.Context, chunks []Chunk) (*GraphExtraction, error) {
	entities := make([]EntityCandidate, 0, len(chunks))
	for _, c := range chunks {
		entities = append(entities, EntityCandidate{Name: "Entity " + c.DocID, ChunkID: c.ChunkID})
	}
	return &GraphExtraction{Entities: entities}, nil
}

func TestGraphBuilder(t *testing.T) {
	t.Run("sink includes entity-chunk mappings", func(t *testing.T) {
	b := &GraphBuilder{
		Chunker:   staticChunker{},
		Grapher:   staticGrapher{},
		BatchSize: 1,
	}

	docs := []Document{{ID: "doc-a", Text: "hello"}, {ID: "doc-b", Text: "world"}}

	var sinkCalls int
	result, err := b.buildGraph(context.Background(), docs, func(_ context.Context, batch *GraphBuildResult) error {
		sinkCalls++
		require.NotEmpty(t, batch.Entities)
		require.NotEmpty(t, batch.EntityChunkMappings)
		require.Equal(t, batch.Entities[0].ID, batch.EntityChunkMappings[0].EntityID)
		require.Equal(t, batch.Chunks[0].ChunkID, batch.EntityChunkMappings[0].ChunkID)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, sinkCalls)
	require.Len(t, result.EntityChunkMappings, 2)
	})
}
