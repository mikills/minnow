package kb

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/mikills/minnow/kb/cacheevict"
)

const defaultCacheEvictionRetryWindow = 150 * time.Millisecond

const defaultCacheEvictionRetryTick = 25 * time.Millisecond

func (l *KB) evictCacheIfNeeded(ctx context.Context, protectedEntry string) error {
	l.cacheReserveMu.Lock()
	reserved := l.cacheReservedBytes
	l.cacheReserveMu.Unlock()
	return l.evictCacheProjected(ctx, protectedEntry, reserved)
}

func (l *KB) evictCacheProjected(ctx context.Context, protectedEntry string, incomingBytes int64) error {
	result, budgetExceeded, err := cacheevict.RetryUntilWithinBudget(cacheevict.RetryConfig{
		Window: defaultCacheEvictionRetryWindow,
		Tick:   defaultCacheEvictionRetryTick,
		Now:    l.Clock.Now,
		Sleep: func(d time.Duration) error {
			return sleepWithContext(ctx, d)
		},
		Sweep: func() cacheevict.SweepResult {
			result := l.evictCacheSweepOnce(protectedEntry, incomingBytes)
			l.recordCacheUsage(result.CurrentBytes, result.HeldBytes)
			return result
		},
	})
	if err != nil {
		return err
	}
	if !budgetExceeded {
		return nil
	}
	l.recordCacheBudgetExceeded()
	return fmt.Errorf(
		"%w: current_bytes=%d max_bytes=%d protected_entries=%d held_bytes=%d",
		ErrCacheBudgetExceeded,
		result.CurrentBytes,
		result.MaxBytes,
		result.ProtectedEntries,
		result.HeldBytes,
	)
}

func (l *KB) cacheEntryHoldsLiveState(kbID string) bool {
	return HasPendingSession(filepath.Join(l.CacheDir, kbID))
}

// maxShardHitEntries bounds the hit map with halving pressure: past it
// counts halve and only zero-count entries drop, so stale entries cool off.
const maxShardHitEntries = 8192

// shardHitDecayInterval bounds guarded promotion in time: counts halve at
// most once per interval, so old heat can't permanently outrank recent use.
// Time-based, not per-sweep, so bulk ingest can't fast-forward the clock.
const shardHitDecayInterval = 10 * time.Minute

// RecordShardHit notes one open of a local shard path; safe for concurrent use.
func (l *KB) RecordShardHit(path string) {
	if path == "" {
		return
	}
	l.shardHitMu.Lock()
	defer l.shardHitMu.Unlock()
	if l.shardHitCounts == nil {
		l.shardHitCounts = map[string]uint64{}
	}
	if len(l.shardHitCounts) == 0 {
		// Fresh generation after a drain: restart the decay window so a
		// stale clock never halves new counts on their first sweep.
		l.shardHitLastDecay = nowFrom(l.Clock)
	}
	l.shardHitCounts[path]++
	if len(l.shardHitCounts) > maxShardHitEntries {
		for p, n := range l.shardHitCounts {
			if n >>= 1; n == 0 {
				delete(l.shardHitCounts, p)
			} else {
				l.shardHitCounts[p] = n
			}
		}
	}
}

// snapshotShardHitCounts copies the hit map for one sweep, decaying counts
// at most once per shardHitDecayInterval. The copy keeps eviction from
// holding the counter lock during filesystem work.
func (l *KB) snapshotShardHitCounts() map[string]uint64 {
	l.shardHitMu.Lock()
	defer l.shardHitMu.Unlock()
	now := nowFrom(l.Clock)
	if l.shardHitLastDecay.IsZero() {
		// Backstop; RecordShardHit normally seeds the clock, so this only
		// skips one penalty.
		l.shardHitLastDecay = now
	} else if now.Sub(l.shardHitLastDecay) >= shardHitDecayInterval {
		l.shardHitLastDecay = now
		for p, n := range l.shardHitCounts {
			if n >>= 1; n == 0 {
				delete(l.shardHitCounts, p)
			} else {
				l.shardHitCounts[p] = n
			}
		}
	}
	if len(l.shardHitCounts) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(l.shardHitCounts))
	for p, n := range l.shardHitCounts {
		out[p] = n
	}
	return out
}

// forgetShardHit drops the count for an evicted path so a redownloaded
// shard re-earns protection instead of resurrecting ancient history.
func (l *KB) forgetShardHit(path string) {
	if path == "" {
		return
	}
	l.shardHitMu.Lock()
	defer l.shardHitMu.Unlock()
	delete(l.shardHitCounts, path)
}

func (l *KB) evictCacheSweepOnce(protectedEntry string, incomingBytes int64) cacheevict.SweepResult {
	l.mu.Lock()
	maxBytes := l.MaxCacheBytes
	entryTTL := l.CacheEntryTTL
	highWatermark := l.CacheHighWatermarkPercent
	lowWatermark := l.CacheLowWatermarkPercent
	minFreeBytes := l.CacheMinFreeBytes
	l.mu.Unlock()
	result := cacheevict.Sweep(cacheevict.Config{
		Root:                 l.CacheDir,
		MaxBytes:             maxBytes,
		TTL:                  entryTTL,
		HighWatermarkPercent: highWatermark,
		LowWatermarkPercent:  lowWatermark,
		MinFreeBytes:         minFreeBytes,
		IncomingBytes:        incomingBytes,
		Protected:            protectedCacheEntries(protectedEntry),
		HitCounts:            l.snapshotShardHitCounts(),
		Unevictable:          l.cacheEntryHoldsLiveState,
		Now:                  l.Clock.Now(),
		Remove: func(entry cacheevict.Entry, reason cacheevict.Reason) bool {
			return l.removeCacheEntry(entry, false)
		},
	})
	for i := 0; i < result.TTLEvictions; i++ {
		l.recordCacheEvictionTTL()
	}
	for i := 0; i < result.SizeEvictions; i++ {
		l.recordCacheEvictionSize()
	}
	for i := 0; i < result.ProbationEvictions; i++ {
		l.recordCacheEvictionProbation()
	}
	for i := 0; i < result.GuardedEvictions; i++ {
		l.recordCacheEvictionGuarded()
	}
	for i := 0; i < result.RemoveErrors; i++ {
		l.recordCacheEvictionError()
	}
	return result
}

type PooledConnCloser interface {
	ClosePooledConns(pathPrefix string)
}

type PooledConnEvictionBarrier interface {
	BeginPooledConnEviction(pathPrefix string) func()
}

// removeCacheEntry skips unpublished rows unless the caller is deleting the
// knowledge base on purpose. entry.Path is a shard file, so the marker lives
// one level up.
func (l *KB) removeCacheEntry(entry cacheevict.Entry, force bool) bool {
	if !force && HasPendingSession(filepath.Join(l.CacheDir, entry.KBID)) {
		return false
	}
	lock := l.LockFor(entry.KBID)
	// Never wait while a mutation may itself be requesting cache eviction;
	// fixed lock stripes can otherwise self-deadlock or cross-deadlock. A busy
	// entry is skipped and reconsidered by the next sweep.
	if !lock.TryLock() {
		return false
	}
	defer lock.Unlock()

	formats := l.registeredFormatsSnapshot()
	var releases []func()
	for _, format := range formats {
		if barrier, ok := format.(PooledConnEvictionBarrier); ok {
			releases = append(releases, barrier.BeginPooledConnEviction(entry.Path))
			continue
		}
		if closer, ok := format.(PooledConnCloser); ok {
			closer.ClosePooledConns(entry.Path)
		}
	}
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()

	removed := removeCacheDir(entry.Path)
	if removed {
		l.forgetShardHit(entry.Path)
	}
	return removed
}

func removeCacheDir(path string) bool {
	return os.RemoveAll(path) == nil
}

func protectedCacheEntries(explicitProtect string) map[string]bool {
	protected := map[string]bool{}
	if explicitProtect != "" {
		protected[explicitProtect] = true
	}
	return protected
}

func (l *KB) recordCacheUsage(current, held int64) {
	l.mu.Lock()
	l.cacheBytesCurrent = current
	l.cacheHeldBytes = held
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionTTL() {
	l.mu.Lock()
	l.cacheEvictionsTTLTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionSize() {
	l.mu.Lock()
	l.cacheEvictionsSizeTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionProbation() {
	l.mu.Lock()
	l.cacheEvictionsProbationTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionGuarded() {
	l.mu.Lock()
	l.cacheEvictionsGuardedTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheEvictionError() {
	l.mu.Lock()
	l.cacheEvictionErrorsTotal++
	l.mu.Unlock()
}

func (l *KB) recordCacheBudgetExceeded() {
	l.mu.Lock()
	l.cacheBudgetExceededTotal++
	l.mu.Unlock()
}

func (l *KB) SweepCache(ctx context.Context) error {
	return l.evictCacheIfNeeded(ctx, "")
}

type CacheEvictionMetricsSnapshot = cacheevict.MetricsSnapshot

func (l *KB) CacheEvictionMetricsSnapshot() CacheEvictionMetricsSnapshot {
	l.mu.Lock()
	snapshot := CacheEvictionMetricsSnapshot{
		CacheBytesCurrent:            l.cacheBytesCurrent,
		CacheHeldBytes:               l.cacheHeldBytes,
		CacheEvictionsTTLTotal:       l.cacheEvictionsTTLTotal,
		CacheEvictionsSizeTotal:      l.cacheEvictionsSizeTotal,
		CacheEvictionsProbationTotal: l.cacheEvictionsProbationTotal,
		CacheEvictionsGuardedTotal:   l.cacheEvictionsGuardedTotal,
		CacheEvictionErrorsTotal:     l.cacheEvictionErrorsTotal,
		CacheBudgetExceededTotal:     l.cacheBudgetExceededTotal,
	}
	cacheDir := l.CacheDir
	l.mu.Unlock()
	if usage, err := cacheevict.MeasureDiskUsage(cacheDir); err == nil {
		snapshot.DiskCapacityBytes = usage.CapacityBytes
		snapshot.DiskAvailableBytes = usage.AvailableBytes
	}
	return snapshot
}

func (l *KB) CacheEvictionOpenMetricsText() string {
	metrics := cacheevict.OpenMetricsText(l.CacheEvictionMetricsSnapshot())
	if shardMetrics := l.ShardingOpenMetricsText(); shardMetrics != "" {
		metrics += shardMetrics
	}
	return metrics
}

func NewCacheOpenMetricsHandler(kb *KB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if kb == nil {
			http.Error(w, "kb is nil", http.StatusServiceUnavailable)
			return
		}
		metrics := kb.CacheEvictionOpenMetricsText()
		if reporter, ok := kb.BlobStore.(interface {
			ReplicationOpenMetrics(context.Context) (string, error)
		}); ok {
			replicationMetrics, err := reporter.ReplicationOpenMetrics(r.Context())
			if err != nil {
				http.Error(w, "replication metrics unavailable", http.StatusServiceUnavailable)
				return
			}
			metrics += replicationMetrics
		}
		w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(metrics))
	})
}
