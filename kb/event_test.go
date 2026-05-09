package kb_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mikills/minnow/kb"

	"github.com/stretchr/testify/require"
)

func mkEvent(id, kbID string, kind kb.EventKind, idem string) kb.KBEvent {
	return kb.KBEvent{
		EventID:        id,
		KBID:           kbID,
		Kind:           kind,
		IdempotencyKey: idem,
		CorrelationID:  "corr-" + id,
	}
}

// nonTransactionalStore wraps an kb.InMemoryEventStore and hides the
// InTransaction method so it no longer satisfies kb.TransactionRunner. Used to
// verify kb.NewWorkerPool rejects it.
type nonTransactionalStore struct{ inner *kb.InMemoryEventStore }

func (n nonTransactionalStore) Append(ctx context.Context, e kb.KBEvent) error {
	return n.inner.Append(ctx, e)
}

func (n nonTransactionalStore) Claim(
	ctx context.Context,
	k kb.EventKind,
	w string,
	v time.Duration,
) (*kb.KBEvent, error) {
	return n.inner.Claim(ctx, k, w, v)
}
func (n nonTransactionalStore) Ack(ctx context.Context, id string) error { return n.inner.Ack(ctx, id) }
func (n nonTransactionalStore) Fail(ctx context.Context, id string, a int, msg string) error {
	return n.inner.Fail(ctx, id, a, msg)
}
func (n nonTransactionalStore) Requeue(ctx context.Context, now time.Time) (int, error) {
	return n.inner.Requeue(ctx, now)
}
func (n nonTransactionalStore) Get(ctx context.Context, id string) (*kb.KBEvent, error) {
	return n.inner.Get(ctx, id)
}

func (n nonTransactionalStore) FindByIdempotency(
	ctx context.Context,
	k kb.EventKind,
	kb, key string,
) (*kb.KBEvent, error) {
	return n.inner.FindByIdempotency(ctx, k, kb, key)
}
func (n nonTransactionalStore) FindByCausation(ctx context.Context, k kb.EventKind, id string) (*kb.KBEvent, error) {
	return n.inner.FindByCausation(ctx, k, id)
}

func (n nonTransactionalStore) ListUnfinishedBefore(
	ctx context.Context,
	k kb.EventKind,
	before time.Time,
	after string,
	limit int,
) ([]kb.KBEvent, string, error) {
	return n.inner.ListUnfinishedBefore(ctx, k, before, after, limit)
}
func (n nonTransactionalStore) Cleanup(ctx context.Context, olderThan time.Time) (int, error) {
	return n.inner.Cleanup(ctx, olderThan)
}

// appendFailSpy wraps an kb.InMemoryEventStore, recording the order of Append
// and Fail calls, optionally forcing Append to error for events of a given
// kind. Used to prove worker.recordFailure's ordering contract.
type appendFailSpy struct {
	inner          *kb.InMemoryEventStore
	calls          *[]string
	failAppendKind string
}

func (a *appendFailSpy) record(s string) {
	if a.calls != nil {
		*a.calls = append(*a.calls, s)
	}
}

func (a *appendFailSpy) Append(ctx context.Context, e kb.KBEvent) error {
	a.record("Append:" + string(e.Kind))
	if a.failAppendKind != "" && string(e.Kind) == a.failAppendKind {
		return errors.New("injected append failure")
	}
	return a.inner.Append(ctx, e)
}
func (a *appendFailSpy) Claim(ctx context.Context, k kb.EventKind, w string, v time.Duration) (*kb.KBEvent, error) {
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
func (a *appendFailSpy) Get(ctx context.Context, id string) (*kb.KBEvent, error) {
	return a.inner.Get(ctx, id)
}
func (a *appendFailSpy) FindByIdempotency(ctx context.Context, k kb.EventKind, kb, key string) (*kb.KBEvent, error) {
	return a.inner.FindByIdempotency(ctx, k, kb, key)
}
func (a *appendFailSpy) FindByCausation(ctx context.Context, k kb.EventKind, id string) (*kb.KBEvent, error) {
	return a.inner.FindByCausation(ctx, k, id)
}

func (a *appendFailSpy) ListUnfinishedBefore(
	ctx context.Context,
	k kb.EventKind,
	before time.Time,
	after string,
	limit int,
) ([]kb.KBEvent, string, error) {
	return a.inner.ListUnfinishedBefore(ctx, k, before, after, limit)
}
func (a *appendFailSpy) Cleanup(ctx context.Context, olderThan time.Time) (int, error) {
	return a.inner.Cleanup(ctx, olderThan)
}
func (a *appendFailSpy) InTransaction(ctx context.Context, fn func(context.Context) error) error {
	return a.inner.InTransaction(ctx, fn)
}

// stubWorker accepts all events. can be configured to fail or panic.
type stubWorker struct {
	kind      kb.EventKind
	id        string
	calls     atomic.Int32
	failNext  atomic.Bool
	panicNext atomic.Bool
}

func (w *stubWorker) Kind() kb.EventKind { return w.kind }
func (w *stubWorker) WorkerID() string   { return w.id }
func (w *stubWorker) Handle(_ context.Context, _ *kb.KBEvent) (kb.WorkerResult, error) {
	w.calls.Add(1)
	if w.panicNext.Load() {
		w.panicNext.Store(false)
		panic("oops")
	}
	if w.failNext.Load() {
		w.failNext.Store(false)
		return kb.WorkerResult{}, errors.New("planned failure")
	}
	return kb.WorkerResult{}, nil
}

func TestWorkerPool(t *testing.T) {
	t.Run("happy path acks event and records inbox marker", func(t *testing.T) {
		store := kb.NewInMemoryEventStore()
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentChunked, id: "w-1"}
		pool, err := kb.NewWorkerPool(
			w,
			store,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond},
		)
		require.NoError(t, err)

		ctx := context.Background()
		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", kb.EventDocumentChunked, "k1")))
		id, err := pool.HandleOnce(ctx)
		require.NoError(t, err)
		require.Equal(t, "e1", id)
		require.EqualValues(t, 1, w.calls.Load())

		got, err := store.Get(ctx, "e1")
		require.NoError(t, err)
		require.Equal(t, kb.EventStatusDone, got.Status)
		require.Equal(t, 1, inbox.Size())
	})

	t.Run("duplicate inbox marker causes commit to abort and event to be acked outside tx", func(t *testing.T) {
		// There is no upfront Processed() check. The handler runs (which is
		// fine - it must be idempotent), the commit transaction fails with
		// kb.ErrInboxDuplicate when it tries to mark the inbox, and the worker
		// acks the source outside the failed transaction so the duplicate
		// stops being redelivered.
		store := kb.NewInMemoryEventStore()
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentChunked, id: "w-1"}
		pool, err := kb.NewWorkerPool(
			w,
			store,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond},
		)
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, inbox.MarkProcessed(ctx, "w-1", "shared-idem", "external"))
		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", kb.EventDocumentChunked, "shared-idem")))

		id, err := pool.HandleOnce(ctx)
		require.NoError(t, err, "duplicate inbox marker is not surfaced as an error")
		require.Equal(t, "e1", id)

		got, _ := store.Get(ctx, "e1")
		require.Equal(t, kb.EventStatusDone, got.Status, "source event is acked via the outside-tx fallback")
	})

	t.Run("retry without processed marker reruns handler", func(t *testing.T) {
		store := kb.NewInMemoryEventStore()
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentChunked, id: "w-1"}
		pool, err := kb.NewWorkerPool(
			w,
			store,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3},
		)
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", kb.EventDocumentChunked, "shared-idem")))
		claimed, err := store.Claim(ctx, kb.EventDocumentChunked, "w-1", 10*time.Millisecond)
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
		store := kb.NewInMemoryEventStore()
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentEmbedded, id: "w-1"}
		w.failNext.Store(true)
		pool, err := kb.NewWorkerPool(
			w,
			store,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3},
		)
		require.NoError(t, err)
		ctx := context.Background()

		require.NoError(t, store.Append(ctx, mkEvent("e1", "kb", kb.EventDocumentEmbedded, "k")))
		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)

		got, _ := store.Get(ctx, "e1")
		require.Equal(t, kb.EventStatusPending, got.Status, "after one failure event returns to pending for retry")
		require.Equal(t, "planned failure", got.LastError)
	})

	t.Run("handler panic is recovered and dead-letters at event-stored MaxAttempts", func(t *testing.T) {
		store := kb.NewInMemoryEventStore()
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentChunked, id: "w-1"}
		w.panicNext.Store(true)
		pool, err := kb.NewWorkerPool(
			w,
			store,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 1},
		)
		require.NoError(t, err)
		ctx := context.Background()
		// MaxAttempts is now stored on the event (M3) so that two workers
		// with divergent configs agree on the ceiling.
		ev := mkEvent("e1", "kb", kb.EventDocumentChunked, "k")
		ev.MaxAttempts = 1
		require.NoError(t, store.Append(ctx, ev))

		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)
		got, _ := store.Get(ctx, "e1")
		require.Equal(t, kb.EventStatusDead, got.Status)
	})

	t.Run("second Start returns kb.ErrAlreadyStarted; Stop resets so later Start works", func(t *testing.T) {
		store := kb.NewInMemoryEventStore()
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentChunked, id: "w-1"}
		pool, err := kb.NewWorkerPool(
			w,
			store,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 2, PollInterval: 5 * time.Millisecond},
		)
		require.NoError(t, err)

		require.NoError(t, pool.Start(context.Background()))
		require.ErrorIs(t, pool.Start(context.Background()), kb.ErrAlreadyStarted)
		pool.Stop()
		pool.Stop()
		require.NoError(t, pool.Start(context.Background()), "Start after Stop must succeed")
		pool.Stop()
	})

	t.Run("kb.NewWorkerPool rejects non-transactional stores", func(t *testing.T) {
		w := &stubWorker{kind: kb.EventDocumentChunked, id: "w-1"}
		_, err := kb.NewWorkerPool(
			w,
			nonTransactionalStore{kb.NewInMemoryEventStore()},
			kb.NewInMemoryEventInbox(),
			kb.WorkerPoolConfig{},
		)
		require.ErrorIs(t, err, kb.ErrStoreNotTransactional)
	})

	t.Run("recordFailure appends worker.failed BEFORE calling Fail (H3)", func(t *testing.T) {
		base := kb.NewInMemoryEventStore()
		var calls []string
		spy := &appendFailSpy{inner: base, calls: &calls}
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentEmbedded, id: "w-1"}
		w.failNext.Store(true)
		pool, err := kb.NewWorkerPool(
			w,
			spy,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3},
		)
		require.NoError(t, err)

		ctx := context.Background()
		ev := mkEvent("e1", "kb", kb.EventDocumentEmbedded, "k")
		ev.MaxAttempts = 3
		require.NoError(t, spy.Append(ctx, ev))
		calls = calls[:0]

		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)

		// First recorded call must be Append (for worker.failed). Fail
		// comes after. If someone reverses the order the test flips.
		require.GreaterOrEqual(t, len(calls), 2)
		require.Equal(t, "Append:worker.failed", calls[0])
		require.Equal(t, "Fail:e1", calls[1])
	})

	t.Run("recordFailure leaves source pending when worker.failed append errors (H3)", func(t *testing.T) {
		base := kb.NewInMemoryEventStore()
		spy := &appendFailSpy{inner: base, failAppendKind: string(kb.EventWorkerFailed)}
		inbox := kb.NewInMemoryEventInbox()
		w := &stubWorker{kind: kb.EventDocumentEmbedded, id: "w-1"}
		w.failNext.Store(true)
		pool, err := kb.NewWorkerPool(
			w,
			spy,
			inbox,
			kb.WorkerPoolConfig{Concurrency: 1, PollInterval: 10 * time.Millisecond, MaxAttempts: 3},
		)
		require.NoError(t, err)

		ctx := context.Background()
		// Give the event one visible claim so it's in claimed state when Fail would be called.
		ev := mkEvent("e1", "kb", kb.EventDocumentEmbedded, "k")
		ev.MaxAttempts = 3
		require.NoError(t, spy.Append(ctx, ev))

		_, err = pool.HandleOnce(ctx)
		require.Error(t, err)

		got, _ := base.Get(ctx, "e1")
		// Handler failed, worker.failed append failed - we must NOT have
		// called Fail (which would advance to Dead at attempt==maxAttempts,
		// here not reached), so the source remains Claimed until the reaper
		// requeues it. Crucially, status is not kb.EventStatusDead.
		require.NotEqual(t, kb.EventStatusDead, got.Status)
	})
}
