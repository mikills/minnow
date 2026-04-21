package kb

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// defaultCacheEvictionRetryWindow is the maximum time to retry eviction before
// giving up and returning ErrCacheBudgetExceeded.
const defaultCacheEvictionRetryWindow = 150 * time.Millisecond

// defaultCacheEvictionRetryTick is the delay between eviction retry attempts
// when the cache is still over budget.
const defaultCacheEvictionRetryTick = 25 * time.Millisecond

// cacheKBEntry represents a single KB's cache directory for eviction decisions.
type cacheKBEntry struct {
	kbID      string    // knowledge base identifier
	path      string    // absolute path to the cache directory
	bytes     int64     // total size in bytes of all files in the directory
	lastTouch time.Time // most recent modification time of any file
}

// cacheSweepResult summarizes the outcome of one eviction sweep.
type cacheSweepResult struct {
	currentBytes     int64 // total cache size after sweep
	maxBytes         int64 // configured maximum cache size (0 = unlimited)
	protectedKBCount int   // number of KBs excluded from eviction
	overBudget       bool  // true if cache still exceeds maxBytes after sweep
}

// evictCacheIfNeeded runs cache eviction with retry until under budget or deadline.
//
// It repeatedly calls evictCacheSweepOnce until either:
//   - The cache is within budget (returns nil)
//   - The retry window expires (returns ErrCacheBudgetExceeded)
//   - The context is cancelled (returns context error)
//
// The protectKBID parameter specifies a KB that should not be evicted (typically
// the KB currently being loaded).
//
// TODO(j):Capacity should be QPS-based instead of correctness-based
func (l *KB) evictCacheIfNeeded(ctx context.Context, protectKBID string) error {
	deadline := l.Clock.Now().Add(defaultCacheEvictionRetryWindow)
	for {
		result := l.evictCacheSweepOnce(protectKBID)
		l.recordCacheBytesCurrent(result.currentBytes)
		if !result.overBudget {
			return nil
		}
		now := l.Clock.Now()
		if !now.Before(deadline) {
			l.recordCacheBudgetExceeded()
			return fmt.Errorf("%w: current_bytes=%d max_bytes=%d protected_kb_count=%d", ErrCacheBudgetExceeded, result.currentBytes, result.maxBytes, result.protectedKBCount)
		}

		wait := defaultCacheEvictionRetryTick
		remaining := time.Until(deadline)
		if remaining < wait {
			wait = remaining
		}
		if err := sleepWithContext(ctx, wait); err != nil {
			return err
		}
	}
}

// evictCacheSweepOnce performs a single eviction sweep.
//
// The sweep proceeds in two phases:
//  1. TTL eviction: remove entries older than CacheEntryTTL (if configured)
//  2. Size eviction: remove oldest entries until total size <= MaxCacheBytes
//
// Entries are sorted by lastTouch time (oldest first) for LRU ordering.
// Protected KBs are never evicted.
func (l *KB) evictCacheSweepOnce(protectKBID string) cacheSweepResult {
	l.mu.Lock()
	maxBytes := l.MaxCacheBytes
	entryTTL := l.CacheEntryTTL
	l.mu.Unlock()
	if maxBytes <= 0 && entryTTL <= 0 {
		return cacheSweepResult{maxBytes: maxBytes, currentBytes: 0, overBudget: false}
	}

	entries, total := l.collectCacheEntries()
	if len(entries) == 0 {
		return cacheSweepResult{maxBytes: maxBytes, currentBytes: total, overBudget: false}
	}
	if entryTTL <= 0 && (maxBytes <= 0 || total <= maxBytes) {
		return cacheSweepResult{maxBytes: maxBytes, currentBytes: total, overBudget: false}
	}

	protected := l.collectProtectedCacheKBIDs(protectKBID)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].lastTouch.Equal(entries[j].lastTouch) {
			return entries[i].kbID < entries[j].kbID
		}
		return entries[i].lastTouch.Before(entries[j].lastTouch)
	})

	// total is derived from a point-in-time filesystem scan; concurrent writes
	// may cause it to drift slightly from actual disk usage between scans.
	var removed map[string]bool
	if entryTTL > 0 {
		now := l.Clock.Now()
		removed = make(map[string]bool)
		for _, entry := range entries {
			if protected[entry.kbID] {
				continue
			}
			if entry.lastTouch.IsZero() || now.Sub(entry.lastTouch) < entryTTL {
				continue
			}
			if l.removeCacheEntry(entry) {
				l.recordCacheEvictionTTL()
				removed[entry.kbID] = true
				total -= entry.bytes
			}
		}
	}

	if maxBytes <= 0 {
		return cacheSweepResult{
			currentBytes:     total,
			maxBytes:         maxBytes,
			protectedKBCount: len(protected),
			overBudget:       false,
		}
	}

	for _, entry := range entries {
		if total <= maxBytes {
			break
		}
		if removed != nil && removed[entry.kbID] {
			continue
		}
		if protected[entry.kbID] {
			continue
		}
		if l.removeCacheEntry(entry) {
			l.recordCacheEvictionSize()
			total -= entry.bytes
		}
	}

	return cacheSweepResult{
		currentBytes:     total,
		maxBytes:         maxBytes,
		protectedKBCount: len(protected),
		overBudget:       total > maxBytes,
	}
}

// PooledConnCloser is an optional interface that ArtifactFormat implementations
// can satisfy to close pooled connections before cache directory removal.
type PooledConnCloser interface {
	ClosePooledConns(pathPrefix string)
}

// removeCacheEntry deletes a KB's cache directory while holding its lock.
// Returns true if removal succeeded, false otherwise (error is recorded as metric).
func (l *KB) removeCacheEntry(entry cacheKBEntry) bool {
	lock := l.LockFor(entry.kbID)
	lock.Lock()
	defer lock.Unlock()

	formats := l.registeredFormatsSnapshot()
	for _, format := range formats {
		if closer, ok := format.(PooledConnCloser); ok {
			closer.ClosePooledConns(entry.path)
		}
	}

	if err := os.RemoveAll(entry.path); err != nil {
		l.recordCacheEvictionError()
		return false
	}
	return true
}

// recordCacheBytesCurrent updates the current cache size metric.
func (l *KB) recordCacheBytesCurrent(v int64) {
	l.mu.Lock()
	l.cacheBytesCurrent = v
	l.mu.Unlock()
}

// recordCacheEvictionTTL increments the TTL-based eviction counter.
func (l *KB) recordCacheEvictionTTL() {
	l.mu.Lock()
	l.cacheEvictionsTTLTotal++
	l.mu.Unlock()
}

// recordCacheEvictionSize increments the size-based eviction counter.
func (l *KB) recordCacheEvictionSize() {
	l.mu.Lock()
	l.cacheEvictionsSizeTotal++
	l.mu.Unlock()
}

// recordCacheEvictionError increments the eviction error counter.
func (l *KB) recordCacheEvictionError() {
	l.mu.Lock()
	l.cacheEvictionErrorsTotal++
	l.mu.Unlock()
}

// recordCacheBudgetExceeded increments the budget exceeded counter.
func (l *KB) recordCacheBudgetExceeded() {
	l.mu.Lock()
	l.cacheBudgetExceededTotal++
	l.mu.Unlock()
}

// SweepCache runs cache eviction using current TTL/size policies.
// This is the public entry point for manual or scheduled cache cleanup.
func (l *KB) SweepCache(ctx context.Context) error {
	return l.evictCacheIfNeeded(ctx, "")
}

// collectCacheEntries scans the cache directory and returns all KB cache entries
// with their sizes and last touch times.
//
// Returns the list of entries and the total size in bytes. Entries that cannot
// be stat'd are skipped. Returns empty results if the cache directory cannot be read.
func (l *KB) collectCacheEntries() ([]cacheKBEntry, int64) {
	items, err := os.ReadDir(l.CacheDir)
	if err != nil {
		return nil, 0
	}

	entries := make([]cacheKBEntry, 0, len(items))
	var total int64
	for _, item := range items {
		if !item.IsDir() {
			continue
		}
		kbPath := filepath.Join(l.CacheDir, item.Name())
		sz, touch, ok := cacheDirStats(kbPath)
		if !ok {
			continue
		}
		entries = append(entries, cacheKBEntry{kbID: item.Name(), path: kbPath, bytes: sz, lastTouch: touch})
		total += sz
	}
	return entries, total
}

// cacheDirStats walks a directory tree and returns total size and latest mtime.
//
// Returns (size, latestMtime, true) on success, or (0, zero, false) if the
// directory cannot be read.
func cacheDirStats(root string) (int64, time.Time, bool) {
	var total int64
	var latest time.Time
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if path == root {
				return err // cannot read root directory; abort
			}
			return nil // skip individual entry errors; continue walking
		}
		info, err := d.Info()
		if err != nil {
			return nil // skip entries we cannot stat
		}
		if !info.IsDir() {
			total += info.Size()
		}
		if info.ModTime().After(latest) {
			latest = info.ModTime()
		}
		return nil
	})

	if err != nil {
		return 0, time.Time{}, false
	}

	if latest.IsZero() {
		latest = time.Now()
	}

	return total, latest, true
}

// collectProtectedCacheKBIDs returns the set of KB IDs that should not be evicted.
//
// Currently only supports explicit protection via the protectKBID parameter.
// Future versions may add additional protection criteria (e.g., recently accessed).
func (l *KB) collectProtectedCacheKBIDs(explicitProtect string) map[string]bool {
	protected := map[string]bool{}
	if explicitProtect != "" {
		protected[explicitProtect] = true
	}
	return protected
}

type CacheEvictionMetricsSnapshot struct {
	CacheBytesCurrent        int64
	CacheEvictionsTTLTotal   uint64
	CacheEvictionsSizeTotal  uint64
	CacheEvictionErrorsTotal uint64
	CacheBudgetExceededTotal uint64
}

func (l *KB) CacheEvictionMetricsSnapshot() CacheEvictionMetricsSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return CacheEvictionMetricsSnapshot{
		CacheBytesCurrent:        l.cacheBytesCurrent,
		CacheEvictionsTTLTotal:   l.cacheEvictionsTTLTotal,
		CacheEvictionsSizeTotal:  l.cacheEvictionsSizeTotal,
		CacheEvictionErrorsTotal: l.cacheEvictionErrorsTotal,
		CacheBudgetExceededTotal: l.cacheBudgetExceededTotal,
	}
}

func (l *KB) CacheEvictionOpenMetricsText() string {
	m := l.CacheEvictionMetricsSnapshot()
	lines := []string{
		"# TYPE minnow_cache_evictions_total counter",
		fmt.Sprintf("minnow_cache_evictions_total{reason=\"ttl\"} %d", m.CacheEvictionsTTLTotal),
		fmt.Sprintf("minnow_cache_evictions_total{reason=\"size\"} %d", m.CacheEvictionsSizeTotal),
		"# TYPE minnow_cache_eviction_errors_total counter",
		fmt.Sprintf("minnow_cache_eviction_errors_total %d", m.CacheEvictionErrorsTotal),
		"# TYPE minnow_cache_budget_exceeded_total counter",
		fmt.Sprintf("minnow_cache_budget_exceeded_total %d", m.CacheBudgetExceededTotal),
		"# TYPE minnow_cache_bytes_current gauge",
		fmt.Sprintf("minnow_cache_bytes_current %d", m.CacheBytesCurrent),
	}
	metrics := strings.Join(lines, "\n") + "\n"
	if shardMetrics := l.ShardingOpenMetricsText(); shardMetrics != "" {
		metrics += shardMetrics
	}
	return metrics
}

// NewCacheOpenMetricsHandler exports cache eviction/runtime metrics.
func NewCacheOpenMetricsHandler(kb *KB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if kb == nil {
			http.Error(w, "kb is nil", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/openmetrics-text; version=1.0.0; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(kb.CacheEvictionOpenMetricsText()))
	})
}
