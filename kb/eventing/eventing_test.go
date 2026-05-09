package eventing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMemory(t *testing.T) {
	t.Run("store lifecycle", func(t *testing.T) {
		ctx := context.Background()
		store := NewInMemoryStore()
		event := Event{EventID: "e1", KBID: "kb", Kind: EventDocumentUpsert, IdempotencyKey: "idem"}
		require.NoError(t, store.Append(ctx, event))
		require.ErrorIs(t, store.Append(ctx, event), ErrDuplicateKey)

		claimed, err := store.Claim(ctx, EventDocumentUpsert, "worker", time.Minute)
		require.NoError(t, err)
		require.Equal(t, EventStatusClaimed, claimed.Status)
		require.NoError(t, store.Ack(ctx, claimed.EventID))

		got, err := store.Get(ctx, claimed.EventID)
		require.NoError(t, err)
		require.Equal(t, EventStatusDone, got.Status)
	})

	t.Run("inbox deduplicates", func(t *testing.T) {
		ctx := context.Background()
		inbox := NewInMemoryEventInbox()
		require.NoError(t, inbox.MarkProcessed(ctx, "worker", "idem", "event"))
		processed, err := inbox.Processed(ctx, "worker", "idem")
		require.NoError(t, err)
		require.True(t, processed)
		require.True(t, IsInboxDuplicate(inbox.MarkProcessed(ctx, "worker", "idem", "event")))
	})
}
