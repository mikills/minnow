package kb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newTestMongoInbox(t *testing.T) *MongoEventInbox {
	t.Helper()
	coll, _ := newTestMongoCollection(t, "inbox")
	s, err := NewMongoEventInbox(context.Background(), coll)
	require.NoError(t, err)
	return s
}

func TestMongoEventInbox(t *testing.T) {
	t.Run("mark processed dedups per (worker, idempotency key)", func(t *testing.T) {
		in := newTestMongoInbox(t)
		ctx := context.Background()
		require.NoError(t, in.MarkProcessed(ctx, "w-1", "idem-a", "e1"))
		processed, err := in.Processed(ctx, "w-1", "idem-a")
		require.NoError(t, err)
		require.True(t, processed)
		err = in.MarkProcessed(ctx, "w-1", "idem-a", "e1")
		require.True(t, IsInboxDuplicate(err))

		// Same idem on a different worker is accepted.
		require.NoError(t, in.MarkProcessed(ctx, "w-2", "idem-a", "e1"))
	})

	t.Run("cleanup evicts records older than cutoff", func(t *testing.T) {
		in := newTestMongoInbox(t)
		ctx := context.Background()
		require.NoError(t, in.MarkProcessed(ctx, "w", "old", "e1"))
		require.NoError(t, in.MarkProcessed(ctx, "w", "new", "e2"))

		// Back-date the "old" row by patching processed_at.
		filter := map[string]any{"_id": "w|old"}
		update := map[string]any{"$set": map[string]any{"processed_at": time.Now().Add(-30 * 24 * time.Hour)}}
		_, err := in.Collection.UpdateOne(ctx, filter, update)
		require.NoError(t, err)

		n, err := in.Cleanup(ctx, time.Now().Add(-7*24*time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, n)
	})
}
