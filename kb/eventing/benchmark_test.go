package eventing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkInMemoryStoreClaim(b *testing.B) {
	for _, pending := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("pending=%d", pending), func(b *testing.B) {
			ctx := context.Background()
			store := NewInMemoryStore()
			seedInMemoryStoreEvents(b, store, pending)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				event, err := store.Claim(ctx, EventDocumentUpsert, "worker", time.Minute)
				require.NoError(b, err)
				require.NoError(b, store.Fail(ctx, event.EventID, event.Attempt, "retry"))
			}
		})
	}
}

func seedInMemoryStoreEvents(b *testing.B, store *InMemoryStore, count int) {
	b.Helper()
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0)
	for i := range count {
		require.NoError(b, store.Append(ctx, Event{
			EventID:     fmt.Sprintf("evt-%06d", i),
			Kind:        EventDocumentUpsert,
			KBID:        "kb",
			Status:      EventStatusPending,
			CreatedAt:   base.Add(time.Duration(i) * time.Second),
			MaxAttempts: 1000000,
		}))
	}
}
