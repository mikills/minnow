package mutationretry

import (
	"context"
	"time"
)

type Stats struct {
	Operation       string
	Attempts        int
	ConflictCount   int
	TotalRetryDelay time.Duration
	Success         bool
}

type Observer interface{ ObserveMutationRetry(stats Stats) }

type ObserverFunc func(stats Stats)

func (f ObserverFunc) ObserveMutationRetry(stats Stats) {
	if f != nil {
		f(stats)
	}
}

type RunConfig struct {
	Operation  string
	MaxRetries int
	Observer   Observer
	Conflict   func(error) bool
	Op         func() error
}

func Run(ctx context.Context, cfg RunConfig) error {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	stats := Stats{Operation: cfg.Operation}
	for {
		stats.Attempts++
		err := cfg.Op()
		if err == nil {
			stats.Success = true
			notify(cfg.Observer, stats)
			return nil
		}
		if !cfg.Conflict(err) {
			notify(cfg.Observer, stats)
			return err
		}
		stats.ConflictCount++
		if stats.ConflictCount > cfg.MaxRetries {
			notify(cfg.Observer, stats)
			return err
		}
		backoff := time.Duration(stats.ConflictCount*stats.ConflictCount) * 10 * time.Millisecond
		if backoff <= 0 {
			backoff = 10 * time.Millisecond
		}
		stats.TotalRetryDelay += backoff
		if err := Sleep(ctx, backoff); err != nil {
			notify(cfg.Observer, stats)
			return err
		}
	}
}

func notify(observer Observer, stats Stats) {
	if observer != nil {
		observer.ObserveMutationRetry(stats)
	}
}

func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
