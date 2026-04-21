package kb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mkEvent(id, kbID string, kind EventKind, idem string) KBEvent {
	return KBEvent{
		EventID:        id,
		KBID:           kbID,
		Kind:           kind,
		IdempotencyKey: idem,
		CorrelationID:  "corr-" + id,
	}
}

func TestInMemoryEventStore(t *testing.T) {
	t.Run("append rejects duplicate idempotency key per (kb_id, kind)", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()

		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb1", EventDocumentUpsert, "idem-1")))
		err := s.Append(ctx, mkEvent("e2", "kb1", EventDocumentUpsert, "idem-1"))
		require.ErrorIs(t, err, ErrEventDuplicateKey)

		require.NoError(t, s.Append(ctx, mkEvent("e3", "kb2", EventDocumentUpsert, "idem-1")), "different kb_id is not a duplicate")
		require.NoError(t, s.Append(ctx, mkEvent("e4", "kb1", EventMediaUpload, "idem-1")), "different kind is not a duplicate")
	})

	t.Run("claim then ack moves event through pending -> claimed -> done", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb1", EventDocumentChunked, "k1")))
		require.NoError(t, s.Append(ctx, mkEvent("e2", "kb1", EventDocumentChunked, "k2")))

		first, err := s.Claim(ctx, EventDocumentChunked, "worker-1", time.Minute)
		require.NoError(t, err)
		require.Equal(t, "e1", first.EventID, "older event claimed first")
		require.Equal(t, EventStatusClaimed, first.Status)
		require.Equal(t, 1, first.Attempt)

		require.NoError(t, s.Ack(ctx, first.EventID))
		got, err := s.Get(ctx, first.EventID)
		require.NoError(t, err)
		require.Equal(t, EventStatusDone, got.Status)

		second, err := s.Claim(ctx, EventDocumentChunked, "worker-1", time.Minute)
		require.NoError(t, err)
		require.Equal(t, "e2", second.EventID)

		_, err = s.Claim(ctx, EventDocumentChunked, "worker-1", time.Minute)
		require.ErrorIs(t, err, ErrEventNoneAvailable)
	})

	t.Run("fail past max attempts dead-letters the event", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()
		ev := mkEvent("e1", "kb1", EventDocumentEmbedded, "k")
		ev.MaxAttempts = 3
		require.NoError(t, s.Append(ctx, ev))

		for attempt := 1; attempt <= 3; attempt++ {
			claimed, err := s.Claim(ctx, EventDocumentEmbedded, "w", time.Minute)
			require.NoError(t, err)
			require.Equal(t, attempt, claimed.Attempt)
			require.NoError(t, s.Fail(ctx, "e1", claimed.Attempt, "bang"))
		}

		got, err := s.Get(ctx, "e1")
		require.NoError(t, err)
		require.Equal(t, EventStatusDead, got.Status)
		require.Equal(t, "bang", got.LastError)
	})

	t.Run("reaper returns expired claims to pending", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb1", EventDocumentUpsert, "k")))

		claimed, err := s.Claim(ctx, EventDocumentUpsert, "w", 10*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, EventStatusClaimed, claimed.Status)

		// Simulate worker death: visibility timeout passes, reaper runs.
		count, err := s.Requeue(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, count)

		got, err := s.Get(ctx, "e1")
		require.NoError(t, err)
		require.Equal(t, EventStatusPending, got.Status)
		require.Empty(t, got.ClaimedBy)
	})

	t.Run("reaper requeues absurdly far-future ClaimedUntil (M1 clock skew)", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb", EventDocumentUpsert, "k")))
		c, err := s.Claim(ctx, EventDocumentUpsert, "w", time.Minute)
		require.NoError(t, err)

		// Simulate clock skew: some peer wrote a ClaimedUntil far past the
		// believable future. Without the skew guard, this event is trapped.
		s.mu.Lock()
		s.events[c.EventID].ClaimedUntil = time.Now().Add(365 * 24 * time.Hour)
		s.mu.Unlock()

		n, err := s.Requeue(ctx, time.Now())
		require.NoError(t, err)
		require.Equal(t, 1, n)
		got, _ := s.Get(ctx, c.EventID)
		require.Equal(t, EventStatusPending, got.Status)
	})

	t.Run("Fail rejects stale observed attempt (H6)", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e1", "kb", EventDocumentEmbedded, "k")))

		c1, err := s.Claim(ctx, EventDocumentEmbedded, "w1", 10*time.Millisecond)
		require.NoError(t, err)

		// Reaper comes in and requeues; another worker re-claims, advancing attempt.
		_, err = s.Requeue(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)
		_, err = s.Claim(ctx, EventDocumentEmbedded, "w2", time.Minute)
		require.NoError(t, err)

		// First worker tries to Fail with its stale attempt observation.
		err = s.Fail(ctx, "e1", c1.Attempt, "boom")
		require.ErrorIs(t, err, ErrEventStateChanged)
	})

	t.Run("Cleanup removes terminal events and idempotency index (M2)", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()
		require.NoError(t, s.Append(ctx, mkEvent("e-old", "kb", EventDocumentUpsert, "k1")))
		require.NoError(t, s.Append(ctx, mkEvent("e-new", "kb", EventDocumentUpsert, "k2")))

		// mark old as done; backdate it so Cleanup's cutoff matches
		require.NoError(t, s.Ack(ctx, "e-old"))
		s.mu.Lock()
		s.events["e-old"].CreatedAt = time.Now().Add(-48 * time.Hour)
		s.mu.Unlock()

		n, err := s.Cleanup(ctx, time.Now().Add(-24*time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, n)

		_, err = s.Get(ctx, "e-old")
		require.ErrorIs(t, err, ErrEventNotFound)
		// k1 should now be reusable because we pruned the idempotency index.
		require.NoError(t, s.Append(ctx, mkEvent("e-fresh", "kb", EventDocumentUpsert, "k1")))
	})

	t.Run("ListUnfinishedBefore paginates and emits continuation token", func(t *testing.T) {
		s := NewInMemoryEventStore()
		ctx := context.Background()
		for i := 0; i < 5; i++ {
			ev := mkEvent(fmt.Sprintf("e-%d", i), "kb", EventDocumentUpsert, fmt.Sprintf("k-%d", i))
			ev.CreatedAt = time.Now().Add(time.Duration(i) * time.Millisecond)
			require.NoError(t, s.Append(ctx, ev))
		}
		page, next, err := s.ListUnfinishedBefore(ctx, EventDocumentUpsert, time.Time{}, "", 2)
		require.NoError(t, err)
		require.Len(t, page, 2)
		require.NotEmpty(t, next)

		page2, next2, err := s.ListUnfinishedBefore(ctx, EventDocumentUpsert, time.Time{}, next, 2)
		require.NoError(t, err)
		require.Len(t, page2, 2)
		require.NotEmpty(t, next2)

		page3, next3, err := s.ListUnfinishedBefore(ctx, EventDocumentUpsert, time.Time{}, next2, 2)
		require.NoError(t, err)
		require.Len(t, page3, 1)
		require.Empty(t, next3)
	})
}

func TestInMemoryEventInbox(t *testing.T) {
	t.Run("mark processed dedups per (worker, idempotency key)", func(t *testing.T) {
		in := NewInMemoryEventInbox()
		ctx := context.Background()
		require.NoError(t, in.MarkProcessed(ctx, "worker-1", "idem-1", "e1"))
		processed, err := in.Processed(ctx, "worker-1", "idem-1")
		require.NoError(t, err)
		require.True(t, processed)
		err = in.MarkProcessed(ctx, "worker-1", "idem-1", "e1")
		require.True(t, IsInboxDuplicate(err))

		// Same idempotency key on a different worker is not a dup.
		require.NoError(t, in.MarkProcessed(ctx, "worker-2", "idem-1", "e1"))
	})

	t.Run("cleanup evicts records older than cutoff", func(t *testing.T) {
		in := NewInMemoryEventInbox()
		ctx := context.Background()
		require.NoError(t, in.MarkProcessed(ctx, "w", "old", "e1"))
		require.NoError(t, in.MarkProcessed(ctx, "w", "new", "e2"))

		// Force the "old" record to be older.
		in.mu.Lock()
		rec := in.records[inboxRecordKey("w", "old")]
		rec.processedAt = time.Now().Add(-30 * 24 * time.Hour)
		in.records[inboxRecordKey("w", "old")] = rec
		in.mu.Unlock()

		count, err := in.Cleanup(ctx, time.Now().Add(-7*24*time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, count)
		require.Equal(t, 1, in.Size())
	})
}

// nonTransactionalStore wraps an InMemoryEventStore and hides the
// InTransaction method so it no longer satisfies TransactionRunner. Used to
// verify NewWorkerPool rejects it.
type nonTransactionalStore struct{ inner *InMemoryEventStore }

func (n nonTransactionalStore) Append(ctx context.Context, e KBEvent) error {
	return n.inner.Append(ctx, e)
}
func (n nonTransactionalStore) Claim(ctx context.Context, k EventKind, w string, v time.Duration) (*KBEvent, error) {
	return n.inner.Claim(ctx, k, w, v)
}
func (n nonTransactionalStore) Ack(ctx context.Context, id string) error { return n.inner.Ack(ctx, id) }
func (n nonTransactionalStore) Fail(ctx context.Context, id string, a int, msg string) error {
	return n.inner.Fail(ctx, id, a, msg)
}
func (n nonTransactionalStore) Requeue(ctx context.Context, now time.Time) (int, error) {
	return n.inner.Requeue(ctx, now)
}
func (n nonTransactionalStore) Get(ctx context.Context, id string) (*KBEvent, error) {
	return n.inner.Get(ctx, id)
}
func (n nonTransactionalStore) FindByIdempotency(ctx context.Context, k EventKind, kb, key string) (*KBEvent, error) {
	return n.inner.FindByIdempotency(ctx, k, kb, key)
}
func (n nonTransactionalStore) FindByCausation(ctx context.Context, k EventKind, id string) (*KBEvent, error) {
	return n.inner.FindByCausation(ctx, k, id)
}
func (n nonTransactionalStore) ListUnfinishedBefore(ctx context.Context, k EventKind, before time.Time, after string, limit int) ([]KBEvent, string, error) {
	return n.inner.ListUnfinishedBefore(ctx, k, before, after, limit)
}
func (n nonTransactionalStore) Cleanup(ctx context.Context, olderThan time.Time) (int, error) {
	return n.inner.Cleanup(ctx, olderThan)
}

// appendFailSpy wraps an InMemoryEventStore, recording the order of Append
// and Fail calls, optionally forcing Append to error for events of a given
// kind. Used to prove worker.recordFailure's ordering contract.
type appendFailSpy struct {
	inner          *InMemoryEventStore
	calls          *[]string
	failAppendKind string
}

func (a *appendFailSpy) record(s string) {
	if a.calls != nil {
		*a.calls = append(*a.calls, s)
	}
}

func (a *appendFailSpy) Append(ctx context.Context, e KBEvent) error {
	a.record("Append:" + string(e.Kind))
	if a.failAppendKind != "" && string(e.Kind) == a.failAppendKind {
		return errors.New("injected append failure")
	}
	return a.inner.Append(ctx, e)
}
func (a *appendFailSpy) Claim(ctx context.Context, k EventKind, w string, v time.Duration) (*KBEvent, error) {
	return a.inner.Claim(ctx, k, w, v)
}
func (a *appendFailSpy) Ack(ctx context.Context, id string) error { return a.inner.Ack(ctx, id) }
func (a *appendFailSpy) Fail(ctx context.Context, id string, obs int, msg string) error {
	a.record("Fail:" + id)
	return a.inner.Fail(ctx, id, obs, msg)
}
func (a *appendFailSpy) Requeue(ctx context.Context, now time.Time) (int, error) {
	return a.inner.Requeue(ctx, now)
}
func (a *appendFailSpy) Get(ctx context.Context, id string) (*KBEvent, error) {
	return a.inner.Get(ctx, id)
}
func (a *appendFailSpy) FindByIdempotency(ctx context.Context, k EventKind, kb, key string) (*KBEvent, error) {
	return a.inner.FindByIdempotency(ctx, k, kb, key)
}
func (a *appendFailSpy) FindByCausation(ctx context.Context, k EventKind, id string) (*KBEvent, error) {
	return a.inner.FindByCausation(ctx, k, id)
}
func (a *appendFailSpy) ListUnfinishedBefore(ctx context.Context, k EventKind, before time.Time, after string, limit int) ([]KBEvent, string, error) {
	return a.inner.ListUnfinishedBefore(ctx, k, before, after, limit)
}
func (a *appendFailSpy) Cleanup(ctx context.Context, olderThan time.Time) (int, error) {
	return a.inner.Cleanup(ctx, olderThan)
}
func (a *appendFailSpy) InTransaction(ctx context.Context, fn func(context.Context) error) error {
	return a.inner.InTransaction(ctx, fn)
}

// stubWorker accepts all events; can be configured to fail or panic.
type stubWorker struct {
	kind      EventKind
	id        string
	calls     atomic.Int32
	failNext  atomic.Bool
	panicNext atomic.Bool
}

func (w *stubWorker) Kind() EventKind  { return w.kind }
func (w *stubWorker) WorkerID() string { return w.id }
func (w *stubWorker) Handle(_ context.Context, _ *KBEvent) (WorkerResult, error) {
	w.calls.Add(1)
	if w.panicNext.Load() {
		w.panicNext.Store(false)
		panic("oops")
	}
	if w.failNext.Load() {
		w.failNext.Store(false)
		return WorkerResult{}, errors.New("planned failure")
	}
	return WorkerResult{}, nil
}

func TestWorkerPool(t *testing.T) {
	t.Run("happy path acks event and records inbox marker", func(t *testing.T) {
		store := NewInMemoryEventStore()
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentChunked, id: "w-1"}
		pool, err := NewWorkerPool(w, store, inbox, WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond})
		require.NoError(t, err)

		ctx := context.Background()
		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", EventDocumentChunked, "k1")))
		id, err := pool.HandleOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, "e1", id)
		require.EqualValues(t, 1, w.calls.Load())

		got, err := store.Get(ctx, "e1")
		require.NoError(t, err)
		require.Equal(t, EventStatusDone, got.Status)
		require.Equal(t, 1, inbox.Size())
	})

	t.Run("duplicate inbox marker causes commit to abort and event to be acked outside tx", func(t *testing.T) {
		// There is no upfront Processed() check. The handler runs (which is
		// fine - it must be idempotent), the commit transaction fails with
		// ErrInboxDuplicate when it tries to mark the inbox, and the worker
		// acks the source outside the failed transaction so the duplicate
		// stops being redelivered.
		store := NewInMemoryEventStore()
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentChunked, id: "w-1"}
		pool, err := NewWorkerPool(w, store, inbox, WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond})
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, inbox.MarkProcessed(ctx, "w-1", "shared-idem", "external"))
		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", EventDocumentChunked, "shared-idem")))

		id, err := pool.HandleOnce(ctx)
		require.NoError(t, err, "duplicate inbox marker is not surfaced as an error")
		require.Equal(t, "e1", id)

		got, _ := store.Get(ctx, "e1")
		require.Equal(t, EventStatusDone, got.Status, "source event is acked via the outside-tx fallback")
	})

	t.Run("retry without processed marker reruns handler", func(t *testing.T) {
		store := NewInMemoryEventStore()
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentChunked, id: "w-1"}
		pool, err := NewWorkerPool(w, store, inbox, WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3})
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", EventDocumentChunked, "shared-idem")))
		claimed, err := store.Claim(ctx, EventDocumentChunked, "w-1", 10*time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, "e1", claimed.EventID)

		count, err := store.Requeue(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)
		require.Equal(t, 1, count)

		_, err = pool.HandleOnce(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, w.calls.Load(), "retry must rerun handler when no processed marker exists")
	})

	t.Run("handler failure returns event to pending for retry", func(t *testing.T) {
		store := NewInMemoryEventStore()
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentEmbedded, id: "w-1"}
		w.failNext.Store(true)
		pool, err := NewWorkerPool(w, store, inbox, WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3})
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", EventDocumentEmbedded, "k")))
		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)

		got, _ := store.Get(ctx, "e1")
		require.Equal(t, EventStatusPending, got.Status, "after one failure event returns to pending for retry")
		require.Equal(t, "planned failure", got.LastError)
	})

	t.Run("handler panic is recovered and dead-letters at event-stored MaxAttempts", func(t *testing.T) {
		store := NewInMemoryEventStore()
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentChunked, id: "w-1"}
		w.panicNext.Store(true)
		pool, err := NewWorkerPool(w, store, inbox, WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 1})
		require.NoError(t, err)
		ctx := context.Background()
		// MaxAttempts is now stored on the event (M3) so that two workers
		// with divergent configs agree on the ceiling.
		ev := mkEvent("e1", "kb", EventDocumentChunked, "k")
		ev.MaxAttempts = 1
		require.NoError(t, store.Append(ctx, ev))

		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)
		got, _ := store.Get(ctx, "e1")
		require.Equal(t, EventStatusDead, got.Status)
	})

	t.Run("second Start returns ErrAlreadyStarted; Stop resets so later Start works", func(t *testing.T) {
		store := NewInMemoryEventStore()
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentChunked, id: "w-1"}
		pool, err := NewWorkerPool(w, store, inbox, WorkerPoolConfig{Concurrency: 2, PollInterval: 5 * time.Millisecond})
		require.NoError(t, err)

		require.NoError(t, pool.Start(context.Background()))
		require.ErrorIs(t, pool.Start(context.Background()), ErrAlreadyStarted)
		pool.Stop()
		pool.Stop()
		require.NoError(t, pool.Start(context.Background()), "Start after Stop must succeed")
		pool.Stop()
	})

	t.Run("NewWorkerPool rejects non-transactional stores", func(t *testing.T) {
		w := &stubWorker{kind: EventDocumentChunked, id: "w-1"}
		_, err := NewWorkerPool(w, nonTransactionalStore{NewInMemoryEventStore()}, NewInMemoryEventInbox(), WorkerPoolConfig{})
		require.ErrorIs(t, err, ErrStoreNotTransactional)
	})

	t.Run("recordFailure appends worker.failed BEFORE calling Fail (H3)", func(t *testing.T) {
		base := NewInMemoryEventStore()
		var calls []string
		spy := &appendFailSpy{inner: base, calls: &calls}
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentEmbedded, id: "w-1"}
		w.failNext.Store(true)
		pool, err := NewWorkerPool(w, spy, inbox, WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3})
		require.NoError(t, err)

		ctx := context.Background()
		ev := mkEvent("e1", "kb", EventDocumentEmbedded, "k")
		ev.MaxAttempts = 3
		require.NoError(t, spy.Append(ctx, ev))
		calls = calls[:0]

		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)

		// First recorded call must be Append (for worker.failed); Fail
		// comes after. If someone reverses the order the test flips.
		require.GreaterOrEqual(t, len(calls), 2)
		require.Equal(t, "Append:worker.failed", calls[0])
		require.Equal(t, "Fail:e1", calls[1])
	})

	t.Run("recordFailure leaves source pending when worker.failed append errors (H3)", func(t *testing.T) {
		base := NewInMemoryEventStore()
		spy := &appendFailSpy{inner: base, failAppendKind: string(EventWorkerFailed)}
		inbox := NewInMemoryEventInbox()
		w := &stubWorker{kind: EventDocumentEmbedded, id: "w-1"}
		w.failNext.Store(true)
		pool, err := NewWorkerPool(w, spy, inbox, WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3})
		require.NoError(t, err)

		ctx := context.Background()
		// Give the event one visible claim so it's in claimed state when Fail would be called.
		ev := mkEvent("e1", "kb", EventDocumentEmbedded, "k")
		ev.MaxAttempts = 3
		require.NoError(t, spy.Append(ctx, ev))

		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)

		got, _ := base.Get(ctx, "e1")
		// Handler failed, worker.failed append failed - we must NOT have
		// called Fail (which would advance to Dead at attempt==maxAttempts,
		// here not reached), so the source remains Claimed until the reaper
		// requeues it. Crucially, status is not EventStatusDead.
		require.NotEqual(t, EventStatusDead, got.Status)
	})
}

