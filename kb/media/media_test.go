package media

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMediaPrimitives(t *testing.T) {
	t.Run("sanitize filename", func(t *testing.T) {
		got, err := SanitizeMediaFilename("nested/path/doc.pdf")
		require.NoError(t, err)
		require.Equal(t, "doc.pdf", got)
	})

	t.Run("reject traversal", func(t *testing.T) {
		_, err := SanitizeMediaFilename("../../etc/passwd")
		require.Error(t, err)
	})

	t.Run("blob key", func(t *testing.T) {
		require.Equal(t, "kbs/kb1/media/med1/doc.pdf", MediaBlobKey("kb1", "med1", "doc.pdf"))
	})
}

func TestInMemoryMediaStore(t *testing.T) {
	store := NewInMemoryMediaStore()
	obj := MediaObject{ID: "m1", KBID: "kb1", Filename: "a.txt", State: MediaStatePending, IdempotencyKey: "idem"}
	require.NoError(t, store.Put(context.Background(), obj))

	got, err := store.Get(context.Background(), "m1")
	require.NoError(t, err)
	require.Equal(t, obj.ID, got.ID)

	byKey, err := store.FindByIdempotency(context.Background(), "kb1", "idem")
	require.NoError(t, err)
	require.Equal(t, obj.ID, byKey.ID)

	require.NoError(t, store.UpdateState(context.Background(), "m1", MediaStateActive, 0))
	page, err := store.ListByState(context.Background(), "kb1", MediaStateActive, "", 10)
	require.NoError(t, err)
	require.Len(t, page.Items, 1)
}
