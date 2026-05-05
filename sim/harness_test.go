package sim

import (
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestHarness(t *testing.T) {
	t.Run("ingest search and cache reset", func(t *testing.T) {
		h := New(t, WithSeed(123))
		const kbID = "harness-behavior"
		docs := []kb.Document{{ID: "doc-1", Text: "alpha searchable content"}}
		require.NoError(t, h.Ingest(kbID, docs))
		require.Equal(t, []string{"doc-1"}, h.IngestedDocIDs(kbID))
		vec, err := h.Embed(h.Ctx(), docs[0].Text)
		require.NoError(t, err)
		results, err := h.Search(kbID, vec, 1)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, "doc-1", results[0].ID)
		require.NoError(t, h.WipeCache())
	})
}
