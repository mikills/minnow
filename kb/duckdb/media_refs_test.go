package duckdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestQueryResultMediaRefsField(t *testing.T) {
	qr := kb.QueryResult{ID: "a", Content: "c", Distance: 0.1, MediaRefs: []kb.ChunkMediaRef{{MediaID: "m"}}}
	expanded := kb.ExpandedFromVector([]kb.QueryResult{qr})
	require.Len(t, expanded, 1)
	require.Equal(t, []kb.ChunkMediaRef{{MediaID: "m"}}, expanded[0].MediaRefs)
}
