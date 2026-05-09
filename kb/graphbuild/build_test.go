package graphbuild

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type testChunker struct{}

func (testChunker) Chunk(_ context.Context, docID string, text string) ([]Chunk, error) {
	return []Chunk{{DocID: docID, ChunkID: docID + "-chunk", Text: text}}, nil
}

type testGrapher struct{}

func (testGrapher) Extract(_ context.Context, chunks []Chunk) (*Extraction, error) {
	entities := make([]EntityCandidate, 0, len(chunks))
	for _, chunk := range chunks {
		entities = append(entities, EntityCandidate{Name: strings.ToUpper(chunk.Text), ChunkID: chunk.ChunkID})
	}
	return &Extraction{Entities: entities}, nil
}

func TestBuilder(t *testing.T) {
	t.Run("chunks and canonical entities are built", func(t *testing.T) {
		builder := &Builder{Chunker: testChunker{}, Grapher: testGrapher{}}

		result, err := builder.Build(context.Background(), []Document{{ID: "doc", Text: "entity"}})

		require.NoError(t, err)
		require.Len(t, result.Chunks, 1)
		require.Equal(t, []Entity{{ID: "ENTITY", Name: "ENTITY"}}, result.Entities)
		require.Equal(t, []EntityChunkMapping{{EntityID: "ENTITY", ChunkID: "doc-chunk"}}, result.EntityChunkMappings)
	})
}
