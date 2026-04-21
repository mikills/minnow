package kb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func newTestMongoEventStore(t *testing.T) (*MongoEventStore, *mongo.Client) {
	t.Helper()
	coll, client := newTestMongoCollection(t, "events")
	s, err := NewMongoEventStore(context.Background(), coll, client)
	require.NoError(t, err)
	return s, client
}

func TestMongoEventStore(t *testing.T) {
	t.Run("append enforces unique (kb_id, kind, idempotency_key)", func(t *testing.T) {
		s, _ := newTestMongoEventStore(t)
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb", EventDocumentUpsert, "idem-a")))
		err := s.Append(ctx, mkEvent("e2", "kb", EventDocumentUpsert, "idem-a"))
		require.ErrorIs(t, err, ErrEventDuplicateKey)

		// Different (kind, kb_id) with same idempotency key is fine.
		require.NoError(t, s.Append(ctx, mkEvent("e3", "kb2", EventDocumentUpsert, "idem-a")))
		require.NoError(t, s.Append(ctx, mkEvent("e4", "kb", EventMediaUpload, "idem-a")))
	})

	t.Run("claim then ack moves event through pending -> claimed -> done", func(t *testing.T) {
		s, _ := newTestMongoEventStore(t)
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb", EventDocumentChunked, "k1")))
		require.NoError(t, s.Append(ctx, mkEvent("e2", "kb", EventDocumentChunked, "k2")))

		first, err := s.Claim(ctx, EventDocumentChunked, "w", time.Minute)
		require.NoError(t, err)
		require.Equal(t, EventStatusClaimed, first.Status)
		require.Equal(t, 1, first.Attempt)

		require.NoError(t, s.Ack(ctx, first.EventID))
		got, _ := s.Get(ctx, first.EventID)
		require.Equal(t, EventStatusDone, got.Status)

		_, err = s.Claim(ctx, EventDocumentChunked, "w", time.Minute)
		require.NoError(t, err)
		_, err = s.Claim(ctx, EventDocumentChunked, "w", time.Minute)
		require.ErrorIs(t, err, ErrEventNoneAvailable)
	})

	t.Run("concurrent claims atomically award one winner", func(t *testing.T) {
		s, _ := newTestMongoEventStore(t)
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("solo", "kb", EventDocumentChunked, "solo")))

		var winners atomic.Int32
		var noneAvailable atomic.Int32
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.Claim(ctx, EventDocumentChunked, "w", time.Minute)
				if err == nil {
					winners.Add(1)
					return
				}
				if err == ErrEventNoneAvailable {
					noneAvailable.Add(1)
				}
			}()
		}
		wg.Wait()
		require.EqualValues(t, 1, winners.Load(), "findOneAndUpdate must award the claim to exactly one caller")
		require.EqualValues(t, 9, noneAvailable.Load())
	})

	t.Run("fail past max attempts dead-letters the event", func(t *testing.T) {
		s, _ := newTestMongoEventStore(t)
		ctx := context.Background()
		ev := mkEvent("e1", "kb", EventDocumentEmbedded, "k")
		ev.MaxAttempts = 3
		require.NoError(t, s.Append(ctx, ev))
		for i := 1; i <= 3; i++ {
			claimed, err := s.Claim(ctx, EventDocumentEmbedded, "w", time.Minute)
			require.NoError(t, err)
			require.NoError(t, s.Fail(ctx, "e1", claimed.Attempt, "boom"))
		}
		got, _ := s.Get(ctx, "e1")
		require.Equal(t, EventStatusDead, got.Status)
		require.Equal(t, "boom", got.LastError)
	})

	t.Run("requeue returns expired claims to pending", func(t *testing.T) {
		s, _ := newTestMongoEventStore(t)
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb", EventDocumentUpsert, "k")))
		_, err := s.Claim(ctx, EventDocumentUpsert, "w", time.Millisecond)
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
		n, err := s.Requeue(ctx, time.Now().UTC())
		require.NoError(t, err)
		require.Equal(t, 1, n)

		got, _ := s.Get(ctx, "e1")
		require.Equal(t, EventStatusPending, got.Status)
	})

	t.Run("find by causation returns matching child or nil", func(t *testing.T) {
		s, _ := newTestMongoEventStore(t)
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("src", "kb", EventDocumentUpsert, "src")))
		child := mkEvent("child", "kb", EventKBPublished, "child")
		child.CausationID = "src"
		require.NoError(t, s.Append(ctx, child))

		found, err := s.FindByCausation(ctx, EventKBPublished, "src")
		require.NoError(t, err)
		require.NotNil(t, found)
		require.Equal(t, "child", found.EventID)

		none, err := s.FindByCausation(ctx, EventKBPublished, "no-such-src")
		require.NoError(t, err)
		require.Nil(t, none)
	})

	t.Run("in-transaction commits atomically (or cleanly errors on standalone)", func(t *testing.T) {
		s, client := newTestMongoEventStore(t)
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("src", "kb", EventDocumentUpsert, "src")))

		err := s.InTransaction(ctx, func(ctx context.Context) error {
			if err := s.Append(ctx, mkEvent("child", "kb", EventKBPublished, "child")); err != nil {
				return err
			}
			return s.Ack(ctx, "src")
		})
		if err != nil {
			// Standalone (non-replica-set) Mongo returns a clear error here;
			// integration tests against a real replica set succeed. Accept
			// both outcomes so the test is useful across deployment shapes.
			require.Contains(t, err.Error(), "transaction")
			return
		}

		src, _ := s.Get(ctx, "src")
		require.Equal(t, EventStatusDone, src.Status)
		child, _ := s.Get(ctx, "child")
		require.Equal(t, EventKBPublished, child.Kind)

		require.NoError(t, client.Ping(ctx, nil))
	})
}
