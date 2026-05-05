package kb

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MaxReasonableVisibilityFuture is the outer edge of any believable
// ClaimedUntil. Anything beyond this is almost certainly clock skew or a
// bogus write. the reaper treats it as an expired claim.
const MaxReasonableVisibilityFuture = 1 * time.Hour

// InMemoryEventStore stores events in process memory.
type InMemoryEventStore struct {
	mu        sync.Mutex
	txMu      sync.Mutex
	events    map[string]*KBEvent // event_id → event
	idempKeys map[string]string   // idempotency_key|kind|kbid → event_id
	clock     Clock
}

// NewInMemoryEventStore constructs an empty in-memory event store.
func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{
		events:    make(map[string]*KBEvent),
		idempKeys: make(map[string]string),
		clock:     RealClock,
	}
}

// SetClock replaces the event store's Clock. Safe to call at any point. the
// swap is serialised against other store operations via the primary mutex.
func (s *InMemoryEventStore) SetClock(c Clock) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c == nil {
		s.clock = RealClock
		return
	}
	s.clock = c
}

func (s *InMemoryEventStore) now() time.Time { return nowFrom(s.clock) }

func idempKey(e KBEvent) string {
	return e.IdempotencyKey + "|" + string(e.Kind) + "|" + e.KBID
}

// Append inserts the event. Returns ErrEventDuplicateKey on idempotency
// collision.
func (s *InMemoryEventStore) Append(_ context.Context, event KBEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if event.EventID == "" {
		return ErrInvalidEvent("event_id required")
	}
	if event.IdempotencyKey != "" {
		key := idempKey(event)
		if _, exists := s.idempKeys[key]; exists {
			return ErrEventDuplicateKey
		}
		s.idempKeys[key] = event.EventID
	}

	if event.Status == "" {
		event.Status = EventStatusPending
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now()
	}
	if event.MaxAttempts <= 0 {
		event.MaxAttempts = DefaultEventMaxAttempts
	}

	cp := event
	s.events[event.EventID] = &cp
	return nil
}

// Claim returns one pending event of the kind, transitioning it to claimed.
func (s *InMemoryEventStore) Claim(_ context.Context, kind EventKind, workerID string, visibility time.Duration) (*KBEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()

	// Iterate in deterministic created_at order so older events go first.
	candidates := make([]*KBEvent, 0)
	for _, e := range s.events {
		if e.Kind == kind && e.Status == EventStatusPending {
			candidates = append(candidates, e)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].CreatedAt.Equal(candidates[j].CreatedAt) {
			return candidates[i].EventID < candidates[j].EventID
		}
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})
	if len(candidates) == 0 {
		return nil, ErrEventNoneAvailable
	}

	e := candidates[0]
	e.Status = EventStatusClaimed
	e.ClaimedBy = workerID
	e.ClaimedUntil = now.Add(visibility)
	e.Attempt++

	out := *e
	return &out, nil
}

// Ack marks an event done.
func (s *InMemoryEventStore) Ack(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.events[eventID]
	if !ok {
		return ErrEventNotFound
	}
	e.Status = EventStatusDone
	e.LastError = ""
	return nil
}

// Fail transitions a Claimed event back to Pending (or Dead past stored
// MaxAttempts) only if the stored attempt still equals observedAttempt. A
// mismatch means a concurrent Claim/Requeue advanced the counter. the caller
// should re-read.
func (s *InMemoryEventStore) Fail(_ context.Context, eventID string, observedAttempt int, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.events[eventID]
	if !ok {
		return ErrEventNotFound
	}
	if observedAttempt > 0 && e.Attempt != observedAttempt {
		return ErrEventStateChanged
	}
	e.LastError = errMsg
	max := e.MaxAttempts
	if max <= 0 {
		max = DefaultEventMaxAttempts
	}
	if e.Attempt >= max {
		e.Status = EventStatusDead
		return nil
	}
	e.Status = EventStatusPending
	e.ClaimedBy = ""
	e.ClaimedUntil = time.Time{}
	return nil
}

// Requeue returns claimed events with expired visibility (or an absurdly
// far-future ClaimedUntil that can only be the product of clock skew) back
// to pending.
func (s *InMemoryEventStore) Requeue(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.IsZero() {
		now = s.now()
	}
	skewCutoff := now.Add(MaxReasonableVisibilityFuture)
	count := 0
	for _, e := range s.events {
		if e.Status != EventStatusClaimed {
			continue
		}
		if now.After(e.ClaimedUntil) || e.ClaimedUntil.After(skewCutoff) {
			e.Status = EventStatusPending
			e.ClaimedBy = ""
			e.ClaimedUntil = time.Time{}
			count++
		}
	}
	return count, nil
}

// Get returns the event by id.
func (s *InMemoryEventStore) Get(_ context.Context, eventID string) (*KBEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.events[eventID]
	if !ok {
		return nil, ErrEventNotFound
	}
	cp := *e
	return &cp, nil
}

func (s *InMemoryEventStore) FindByIdempotency(_ context.Context, kind EventKind, kbID, idempotencyKey string) (*KBEvent, error) {
	if idempotencyKey == "" {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	eventID, ok := s.idempKeys[idempotencyKey+"|"+string(kind)+"|"+kbID]
	if !ok {
		return nil, nil
	}
	e, ok := s.events[eventID]
	if !ok {
		return nil, nil
	}
	cp := *e
	return &cp, nil
}

func (s *InMemoryEventStore) ListUnfinishedBefore(_ context.Context, kind EventKind, before time.Time, after string, limit int) ([]KBEvent, string, error) {
	if limit <= 0 {
		limit = 1000
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	matches := s.unfinishedEventsBeforeLocked(kind, before)
	sortEventPage(matches)
	return eventPageAfter(matches, after, limit), eventNextToken(matches, after, limit), nil
}

func (s *InMemoryEventStore) unfinishedEventsBeforeLocked(kind EventKind, before time.Time) []KBEvent {
	matches := make([]KBEvent, 0)
	for _, event := range s.events {
		if unfinishedEventMatches(event, kind, before) {
			matches = append(matches, *event)
		}
	}
	return matches
}

func unfinishedEventMatches(event *KBEvent, kind EventKind, before time.Time) bool {
	if event.Kind != kind {
		return false
	}
	if !before.IsZero() && event.CreatedAt.After(before) {
		return false
	}
	return event.Status != EventStatusDone && event.Status != EventStatusDead
}

func sortEventPage(events []KBEvent) {
	sort.Slice(events, func(i, j int) bool {
		if events[i].CreatedAt.Equal(events[j].CreatedAt) {
			return events[i].EventID < events[j].EventID
		}
		return events[i].CreatedAt.Before(events[j].CreatedAt)
	})
}

func eventPageAfter(events []KBEvent, after string, limit int) []KBEvent {
	start, end := eventPageBounds(events, after, limit)
	return append([]KBEvent(nil), events[start:end]...)
}

func eventNextToken(events []KBEvent, after string, limit int) string {
	_, end := eventPageBounds(events, after, limit)
	if end < len(events) {
		return events[end-1].EventID
	}
	return ""
}

func eventPageBounds(events []KBEvent, after string, limit int) (int, int) {
	start := eventPageStart(events, after)
	end := start + limit
	if end > len(events) {
		end = len(events)
	}
	return start, end
}

func eventPageStart(events []KBEvent, after string) int {
	if after == "" {
		return 0
	}
	for idx, event := range events {
		if event.EventID == after {
			return idx + 1
		}
	}
	return 0
}

// Cleanup removes terminal-state events whose CreatedAt is before olderThan
// and prunes the companion idempotency-key index. Returns the count of events
// removed.
func (s *InMemoryEventStore) Cleanup(_ context.Context, olderThan time.Time) (int, error) {
	if olderThan.IsZero() {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, e := range s.events {
		if e.Status != EventStatusDone && e.Status != EventStatusDead {
			continue
		}
		if !e.CreatedAt.Before(olderThan) {
			continue
		}
		delete(s.events, id)
		if e.IdempotencyKey != "" {
			delete(s.idempKeys, idempKey(*e))
		}
		removed++
	}
	return removed, nil
}

// FindByCausation returns the first event of the given kind whose
// CausationID equals sourceEventID, or nil when none exists.
func (s *InMemoryEventStore) FindByCausation(_ context.Context, kind EventKind, sourceEventID string) (*KBEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldest *KBEvent
	for _, e := range s.events {
		if e.Kind != kind || e.CausationID != sourceEventID {
			continue
		}
		if oldest == nil || e.CreatedAt.Before(oldest.CreatedAt) {
			cp := *e
			oldest = &cp
		}
	}
	return oldest, nil
}

// InTransaction serializes fn against other store operations via txMu so the
// worker's append+ack+mark-inbox sequence cannot interleave with other
// callers. The in-memory store still cannot provide true rollback. if fn
// returns an error the caller is responsible for not having mutated external
// state. This matches the Mongo store's externally visible contract closely
// enough for tests and single-process deployments.
func (s *InMemoryEventStore) InTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	s.txMu.Lock()
	defer s.txMu.Unlock()
	return fn(ctx)
}

// Snapshot returns a copy of all events. For tests and operator queries.
func (s *InMemoryEventStore) Snapshot() []KBEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]KBEvent, 0, len(s.events))
	for _, e := range s.events {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

// ErrInvalidEvent describes an invalid-event-input error.
type ErrInvalidEvent string

func (e ErrInvalidEvent) Error() string { return "event: invalid input: " + string(e) }
