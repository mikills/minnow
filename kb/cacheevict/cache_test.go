package cacheevict

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	t.Run("sweep removes expired entries first", func(t *testing.T) {
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		entries := []Entry{
			{KBID: "old", Bytes: 80, LastTouch: now.Add(-2 * time.Hour)},
			{KBID: "new", Bytes: 80, LastTouch: now.Add(-time.Minute)},
		}
		removed := map[string]Reason{}
		result := Sweep(Config{
			MaxBytes:  100,
			TTL:       time.Hour,
			Protected: map[string]bool{},
			Now:       now,
			Remove: func(entry Entry, reason Reason) bool {
				removed[entry.KBID] = reason
				return true
			},
		})
		require.False(t, result.OverBudget)
		require.Empty(t, removed)

		result = sweepEntries(entries, Config{
			MaxBytes:  100,
			TTL:       time.Hour,
			Protected: map[string]bool{},
			Now:       now,
			Remove: func(entry Entry, reason Reason) bool {
				removed[entry.KBID] = reason
				return true
			},
		})
		require.Equal(t, ReasonTTL, removed["old"])
		require.Equal(t, int64(80), result.CurrentBytes)
		require.False(t, result.OverBudget)
	})

	t.Run("retry stops after budget clears", func(t *testing.T) {
		calls := 0
		result, exceeded, err := RetryUntilWithinBudget(RetryConfig{
			Window: time.Second,
			Tick:   time.Millisecond,
			Now:    time.Now,
			Sleep:  func(time.Duration) error { return nil },
			Sweep: func() SweepResult {
				calls++
				return SweepResult{OverBudget: calls == 1, CurrentBytes: 10, MaxBytes: 5}
			},
		})
		require.NoError(t, err)
		require.False(t, exceeded)
		require.False(t, result.OverBudget)
		require.Equal(t, 2, calls)
	})

	t.Run("scan returns directory sizes", func(t *testing.T) {
		root := t.TempDir()
		path := filepath.Join(root, "kb-a")
		require.NoError(t, os.MkdirAll(path, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(path, "blob"), []byte("abc"), 0o644))
		entries, total := ScanEntries(root)
		require.Len(t, entries, 1)
		require.Equal(t, "kb-a", entries[0].KBID)
		require.Equal(t, int64(3), total)
	})
}

func sweepEntries(entries []Entry, cfg Config) SweepResult {
	SortOldestFirst(entries)
	total := int64(0)
	for _, entry := range entries {
		total += entry.Bytes
	}
	removed, result := removeExpired(cfg, entries, total)
	if cfg.MaxBytes > 0 {
		result.CurrentBytes = removeOverBudget(cfg, entries, removed, result.CurrentBytes, &result)
	}
	result.MaxBytes = cfg.MaxBytes
	result.ProtectedKBCount = len(cfg.Protected)
	result.OverBudget = cfg.MaxBytes > 0 && result.CurrentBytes > cfg.MaxBytes
	return result
}
