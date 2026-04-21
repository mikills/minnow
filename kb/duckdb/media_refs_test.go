package duckdb

import (
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodeMediaRefsRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   []string
	}{
		{"empty", nil},
		{"single", []string{"m1"}},
		{"multi", []string{"m1", "m2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := encodeMediaRefs(tc.in, nil)
			require.NoError(t, err)
			decoded, err := decodeMediaRefs(encoded)
			require.NoError(t, err)
			if len(tc.in) == 0 {
				require.Nil(t, decoded)
				return
			}
			require.Len(t, decoded, len(tc.in))
			for i, id := range tc.in {
				require.Equal(t, id, decoded[i].MediaID)
			}
		})
	}
}

// Placeholder for the ref silo: confirms the type plumbing is in place so
// QueryResult can carry MediaRefs from the SQL scan into the higher-level
// ExpandedResult without further adapters.
func TestQueryResultMediaRefsField(t *testing.T) {
	qr := kb.QueryResult{ID: "a", Content: "c", Distance: 0.1, MediaRefs: []kb.ChunkMediaRef{{MediaID: "m"}}}
	expanded := kb.ExpandedFromVector([]kb.QueryResult{qr})
	require.Len(t, expanded, 1)
	require.Equal(t, []kb.ChunkMediaRef{{MediaID: "m"}}, expanded[0].MediaRefs)
}
