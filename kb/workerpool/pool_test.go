package workerpool

import (
	"context"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/eventing"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	t.Run("pending event is handled once", func(t *testing.T) {
		store := eventing.NewInMemoryStore()
		inbox := eventing.NewInMemoryEventInbox()
		evt := eventing.Event{
			EventID:   "e1",
			KBID:      "kb",
			Kind:      eventing.EventDocumentUpsert,
			Status:    eventing.EventStatusPending,
			CreatedAt: time.Now(),
		}
		require.NoError(t, store.Append(context.Background(), evt))
		worker := &stubWorker{kind: eventing.EventDocumentUpsert, id: "w1"}
		pool, err := NewWorkerPool(worker, store, inbox, WorkerPoolConfig{Concurrency: 1})
		require.NoError(t, err)

		id, err := pool.HandleOnce(context.Background())

		require.NoError(t, err)
		require.Equal(t, "e1", id)
		require.True(t, worker.called)
	})
}

type stubWorker struct {
	kind   eventing.EventKind
	id     string
	called bool
}

func (w *stubWorker) Kind() eventing.EventKind { return w.kind }
func (w *stubWorker) WorkerID() string         { return w.id }
func (w *stubWorker) Handle(context.Context, *eventing.Event) (WorkerResult, error) {
	w.called = true
	return WorkerResult{}, nil
}
