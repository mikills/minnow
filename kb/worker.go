package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// WorkerResult captures the post-handle commit work for a worker attempt.
type WorkerResult struct {
	FollowUps []KBEvent
	Commit    func(context.Context) error
}

// Worker handles events of a single Kind. Handle runs the side-effects and
// returns the follow-up events plus any worker-specific commit-side mutation
// that must land atomically with Ack.
type Worker interface {
	Kind() EventKind
	WorkerID() string
	Handle(ctx context.Context, event *KBEvent) (WorkerResult, error)
}

// WorkerPoolConfig configures a worker pool.
type WorkerPoolConfig struct {
	Concurrency       int
	PollInterval      time.Duration
	VisibilityTimeout time.Duration
	MaxAttempts       int
}

func (c WorkerPoolConfig) withDefaults() WorkerPoolConfig {
	out := c
	if out.Concurrency <= 0 {
		out.Concurrency = 4
	}
	if out.PollInterval <= 0 {
		out.PollInterval = 250 * time.Millisecond
	}
	if out.VisibilityTimeout <= 0 {
		out.VisibilityTimeout = 30 * time.Second
	}
	if out.MaxAttempts <= 0 {
		out.MaxAttempts = 5
	}
	return out
}

// WorkerMetrics observes per-event outcomes.
type WorkerMetrics interface {
	OnWorkerTick(kind EventKind, workerID string, outcome string, duration time.Duration, err error)
}

// NoopWorkerMetrics discards observations.
type NoopWorkerMetrics struct{}

// OnWorkerTick implements WorkerMetrics.
func (NoopWorkerMetrics) OnWorkerTick(_ EventKind, _ string, _ string, _ time.Duration, _ error) {}

// Sentinel errors for WorkerPool construction and lifecycle.
var (
	ErrStoreNotTransactional = errors.New("worker pool: event store must implement TransactionRunner")
	ErrAlreadyStarted        = errors.New("worker pool: already started")
)

// WorkerPool runs goroutines that claim and dispatch events for a single
// Worker.
type WorkerPool struct {
	worker  Worker
	store   EventStore
	runner  TransactionRunner
	inbox   EventInbox
	cfg     WorkerPoolConfig
	metrics WorkerMetrics

	wg     sync.WaitGroup
	cancel context.CancelFunc

	startedMu sync.Mutex
	started   bool
}

// NewWorkerPool constructs a pool around the given worker. The EventStore
// must also implement TransactionRunner, otherwise the pool cannot uphold
// the outbox's atomicity contract and construction returns
// ErrStoreNotTransactional.
func NewWorkerPool(worker Worker, store EventStore, inbox EventInbox, cfg WorkerPoolConfig) (*WorkerPool, error) {
	runner, ok := store.(TransactionRunner)
	if !ok {
		return nil, ErrStoreNotTransactional
	}
	return &WorkerPool{
		worker:  worker,
		store:   store,
		runner:  runner,
		inbox:   inbox,
		cfg:     cfg.withDefaults(),
		metrics: NoopWorkerMetrics{},
	}, nil
}

// SetMetrics attaches a metrics observer. Safe to call before Start.
func (p *WorkerPool) SetMetrics(m WorkerMetrics) {
	if m == nil {
		m = NoopWorkerMetrics{}
	}
	p.metrics = m
}

// Start begins polling. Returns ErrAlreadyStarted if the pool was already
// started and not yet stopped.
func (p *WorkerPool) Start(parentCtx context.Context) error {
	p.startedMu.Lock()
	defer p.startedMu.Unlock()
	if p.started {
		return ErrAlreadyStarted
	}
	p.started = true

	ctx, cancel := context.WithCancel(parentCtx)
	p.cancel = cancel

	for i := 0; i < p.cfg.Concurrency; i++ {
		p.wg.Add(1)
		go p.loop(ctx)
	}
	return nil
}

// Stop signals workers to exit and waits for them. After Stop returns, the
// pool may be started again with Start.
func (p *WorkerPool) Stop() {
	p.startedMu.Lock()
	if !p.started || p.cancel == nil {
		p.startedMu.Unlock()
		return
	}
	cancel := p.cancel
	p.cancel = nil
	p.startedMu.Unlock()

	cancel()
	p.wg.Wait()

	p.startedMu.Lock()
	p.started = false
	p.startedMu.Unlock()
}

func (p *WorkerPool) loop(ctx context.Context) {
	defer p.wg.Done()
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

func (p *WorkerPool) tick(ctx context.Context) {
	for {
		event, err := p.store.Claim(ctx, p.worker.Kind(), p.worker.WorkerID(), p.cfg.VisibilityTimeout)
		if err != nil {
			if errors.Is(err, ErrEventNoneAvailable) {
				return
			}
			slog.Default().Warn("worker claim failed", "kind", string(p.worker.Kind()), "error", err)
			return
		}
		p.dispatch(ctx, event)
	}
}

// HandleOnce claims at most one event and dispatches it. Returns the
// dispatched event id and the handler error (if any).
func (p *WorkerPool) HandleOnce(ctx context.Context) (string, error) {
	event, err := p.store.Claim(ctx, p.worker.Kind(), p.worker.WorkerID(), p.cfg.VisibilityTimeout)
	if err != nil {
		return "", err
	}
	return event.EventID, p.dispatch(ctx, event)
}

func (p *WorkerPool) dispatch(ctx context.Context, event *KBEvent) error {
	start := time.Now()

	// Run side-effects. They must remain idempotent across crashes because
	// until MarkProcessed lands inside the commit transaction, a concurrent
	// worker (or the reaper+next claim) may rerun Handle on the same event.
	result, handlerErr := safeHandle(ctx, p.worker, event)
	if handlerErr != nil {
		p.recordFailure(ctx, event, handlerErr, time.Since(start))
		return handlerErr
	}

	// Commit transactionally. Order matters: follow-ups → commit mutation →
	// Ack source → MarkProcessed (the atomic claim). If any step fails, the
	// inbox marker is never written, so a redelivery still sees "not
	// processed" and retries. Commit mutations (blob deletes) must therefore
	// be idempotent.
	commit := func(ctx context.Context) error {
		for _, up := range result.FollowUps {
			if err := p.store.Append(ctx, up); err != nil && !errors.Is(err, ErrEventDuplicateKey) {
				return fmt.Errorf("append follow-up %s: %w", up.Kind, err)
			}
		}
		if result.Commit != nil {
			if err := result.Commit(ctx); err != nil {
				return err
			}
		}
		if err := p.store.Ack(ctx, event.EventID); err != nil {
			return err
		}
		if event.IdempotencyKey != "" && p.inbox != nil {
			if err := p.inbox.MarkProcessed(ctx, p.worker.WorkerID(), event.IdempotencyKey, event.EventID); err != nil {
				return err
			}
		}
		return nil
	}

	commitErr := p.runner.InTransaction(ctx, commit)
	dur := time.Since(start)

	// If we lost the inbox race, another worker already committed this
	// event. Ack outside the failed transaction so we stop being
	// redelivered and exit cleanly.
	if commitErr != nil && IsInboxDuplicate(commitErr) {
		slog.Default().Info("worker lost inbox race; acking duplicate",
			"kind", string(p.worker.Kind()), "event_id", event.EventID, "idempotency_key", event.IdempotencyKey)
		if err := p.store.Ack(ctx, event.EventID); err != nil && !errors.Is(err, ErrEventNotFound) {
			slog.Default().Warn("worker ack after duplicate failed", "error", err)
		}
		p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), "duplicate", dur, nil)
		return nil
	}

	if commitErr != nil {
		slog.Default().Warn("worker commit failed",
			"kind", string(p.worker.Kind()), "event_id", event.EventID, "error", commitErr)
		p.recordFailure(ctx, event, commitErr, dur)
		return commitErr
	}

	p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), "success", dur, nil)
	return nil
}

// recordFailure appends a worker.failed domain event and then calls
// EventStore.Fail. The ordering matters: if the fail-event append fails
// we must NOT transition the source to Dead, because observers would then
// see "gone dead, no reason". Leaving the source pending lets the next
// visibility-timeout retry try again.
func (p *WorkerPool) recordFailure(ctx context.Context, event *KBEvent, handlerErr error, dur time.Duration) {
	maxAttempts := event.EffectiveMaxAttempts()
	willRetry := event.Attempt < maxAttempts

	var resultCarrier fileResultCarrier
	_ = errors.As(handlerErr, &resultCarrier)
	var fileResults []FileIngestResult
	if resultCarrier != nil {
		fileResults = resultCarrier.FileIngestResults()
	}

	failPayload, _ := json.Marshal(WorkerFailedPayload{
		Stage:         string(p.worker.Kind()),
		SourceEventID: event.EventID,
		Attempt:       event.Attempt,
		Error:         handlerErr.Error(),
		WillRetry:     willRetry,
		FileResults:   fileResults,
	})
	failEvent := KBEvent{
		EventID:        NewULIDLike("evt"),
		KBID:           event.KBID,
		Kind:           EventWorkerFailed,
		Payload:        failPayload,
		PayloadSchema:  "worker.failed/v1",
		CorrelationID:  event.CorrelationID,
		CausationID:    event.EventID,
		IdempotencyKey: fmt.Sprintf("%s|worker.failed|%d", event.EventID, event.Attempt),
		Status:         EventStatusPending,
		CreatedAt:      time.Now().UTC(),
	}
	if err := p.store.Append(ctx, failEvent); err != nil && !errors.Is(err, ErrEventDuplicateKey) {
		slog.Default().Warn("worker.failed append failed; leaving source pending for retry",
			"kind", string(p.worker.Kind()), "event_id", event.EventID, "error", err)
		p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), "failed", dur, handlerErr)
		return
	}

	if err := p.store.Fail(ctx, event.EventID, event.Attempt, handlerErr.Error()); err != nil {
		// ErrEventStateChanged means a concurrent actor advanced the attempt
		// counter; the worker.failed event is in place, so metrics report
		// "failed" and the reaper/next claim will make the terminal call.
		slog.Default().Warn("worker store.Fail failed", "error", err)
	}

	outcome := "failed"
	if !willRetry {
		outcome = "dead"
	}
	p.metrics.OnWorkerTick(p.worker.Kind(), p.worker.WorkerID(), outcome, dur, handlerErr)
}

func safeHandle(ctx context.Context, w Worker, event *KBEvent) (result WorkerResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("worker panicked: %v", r)
		}
	}()
	return w.Handle(ctx, event)
}
