// EventInbox defines the per-worker inbox used to deduplicate at-least-once
// event delivery.
//
// Worker handler protocol:
//  1. Apply side effects outside any transaction. They must be idempotent
//     because a crash before commit leaves them pending a retry.
//  2. Open a single EventStore transaction; inside it, in order:
//     (a) append follow-up events,
//     (b) run the worker's commit-side mutation (blob deletes, etc.),
//     (c) Ack the source event,
//     (d) MarkProcessed(idempotencyKey) - this is the atomic claim.
//
// MarkProcessed inside the transaction doubles as a "first writer wins"
// claim: if two workers ran side-effects concurrently, only one MarkProcessed
// succeeds; the other sees ErrInboxDuplicate and aborts its transaction so
// the follow-ups and ack it staged never land. The worker that lost the race
// treats the source event as already processed and acks it outside the failed
// transaction to drain the queue.
//
// Because MarkProcessed is the claim, there is no longer an upfront
// Processed() check in the hot path; redundant readers made the window
// larger, not smaller.

package kb

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrInboxDuplicate signals that the idempotency key was already processed.
type ErrInboxDuplicate struct {
	IdempotencyKey string
}

func (e *ErrInboxDuplicate) Error() string {
	return "inbox: duplicate idempotency key " + e.IdempotencyKey
}

// IsInboxDuplicate reports whether err is an ErrInboxDuplicate.
func IsInboxDuplicate(err error) bool {
	var d *ErrInboxDuplicate
	return errors.As(err, &d)
}

// EventInbox tracks fully processed idempotency keys per worker, providing a
// durable dedup marker on top of at-least-once delivery.
type EventInbox interface {
	// Processed reports whether the worker has already fully committed the
	// given idempotency key.
	Processed(ctx context.Context, workerID, idempotencyKey string) (bool, error)

	// MarkProcessed records that the worker fully committed the given key.
	// Returns ErrInboxDuplicate if the key was already marked.
	MarkProcessed(ctx context.Context, workerID, idempotencyKey, eventID string) error

	// Cleanup removes inbox rows older than the cutoff. Returns count
	// removed. Driven by the inbox-cleanup sweep.
	Cleanup(ctx context.Context, olderThan time.Time) (int, error)
}

type inboxRecord struct {
	workerID       string
	idempotencyKey string
	eventID        string
	processedAt    time.Time
}

// InMemoryEventInbox is the in-memory EventInbox implementation.
type InMemoryEventInbox struct {
	mu      sync.Mutex
	records map[string]inboxRecord // workerID|idempotencyKey
	clock   Clock
}

// NewInMemoryEventInbox constructs an empty inbox.
func NewInMemoryEventInbox() *InMemoryEventInbox {
	return &InMemoryEventInbox{records: make(map[string]inboxRecord), clock: RealClock}
}

// SetClock replaces the inbox's Clock under the primary mutex.
func (b *InMemoryEventInbox) SetClock(c Clock) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c == nil {
		b.clock = RealClock
		return
	}
	b.clock = c
}

func inboxRecordKey(workerID, idemKey string) string {
	return workerID + "|" + idemKey
}

func (b *InMemoryEventInbox) Processed(_ context.Context, workerID, idempotencyKey string) (bool, error) {
	if idempotencyKey == "" {
		return false, errors.New("inbox: idempotency key required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	_, exists := b.records[inboxRecordKey(workerID, idempotencyKey)]
	return exists, nil
}

// MarkProcessed records a processed key.
func (b *InMemoryEventInbox) MarkProcessed(_ context.Context, workerID, idempotencyKey, eventID string) error {
	if idempotencyKey == "" {
		return errors.New("inbox: idempotency key required")
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	key := inboxRecordKey(workerID, idempotencyKey)
	if _, exists := b.records[key]; exists {
		return &ErrInboxDuplicate{IdempotencyKey: idempotencyKey}
	}
	b.records[key] = inboxRecord{
		workerID:       workerID,
		idempotencyKey: idempotencyKey,
		eventID:        eventID,
		processedAt:    nowFrom(b.clock),
	}
	return nil
}

// Cleanup deletes rows older than the cutoff and returns the count removed.
func (b *InMemoryEventInbox) Cleanup(_ context.Context, olderThan time.Time) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if olderThan.IsZero() {
		return 0, nil
	}
	count := 0
	for key, rec := range b.records {
		if rec.processedAt.Before(olderThan) {
			delete(b.records, key)
			count++
		}
	}
	return count, nil
}

// Size reports the inbox row count. For tests and metrics.
func (b *InMemoryEventInbox) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}
