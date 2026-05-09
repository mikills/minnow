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

type recordingGraphSink struct{ batches []*GraphBuildResult }

func (s *recordingGraphSink) EnsureGraphTables(context.Context) error { return nil }
func (s *recordingGraphSink) InsertGraphBuildResult(_ context.Context, result *GraphBuildResult) error {
	s.batches = append(s.batches, result)
	return nil
}

func TestGraphBuilder(t *testing.T) {
	t.Run("nil builder is rejected", func(t *testing.T) {
		var builder *GraphBuilder

		_, err := builder.Build(context.Background(), nil)

		require.ErrorContains(t, err, "graph builder is nil")
	})

	t.Run("sink includes entity-chunk mappings", func(t *testing.T) {
		builder := &GraphBuilder{Chunker: staticChunker{}, Grapher: staticGrapher{}, BatchSize: 1}
		docs := []Document{{ID: "doc-a", Text: "hello"}, {ID: "doc-b", Text: "world"}}
		sink := &recordingGraphSink{}

		result, err := builder.BuildAndInsert(context.Background(), sink, docs)
		require.NoError(t, err)
		require.Len(t, sink.batches, 2)
		for _, batch := range sink.batches {
			require.NotEmpty(t, batch.Entities)
			require.NotEmpty(t, batch.EntityChunkMappings)
			require.Equal(t, batch.Entities[0].ID, batch.EntityChunkMappings[0].EntityID)
			require.Equal(t, batch.Chunks[0].ChunkID, batch.EntityChunkMappings[0].ChunkID)
		}
		require.Len(t, result.EntityChunkMappings, 2)
	})
}
