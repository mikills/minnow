package cacheevict

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkSweepPlan(b *testing.B) {
	now := time.Unix(1_700_000_000, 0).UTC()
	for _, count := range []int{100, 1000, 10000} {
		entries := benchmarkEntries(count, now)
		b.Run(fmt.Sprintf("entries=%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				copyEntries := append([]Entry(nil), entries...)
				_ = sweepEntries(copyEntries, Config{
					MaxBytes: int64(count/2) * 1024,
					TTL:      time.Hour,
					Now:      now,
					Remove:   func(Entry, Reason) bool { return true },
				})
			}
		})
	}
}

func BenchmarkScanEntries(b *testing.B) {
	for _, count := range []int{100, 1000} {
		root := b.TempDir()
		seedCacheDirs(b, root, count, 4)
		b.Run(fmt.Sprintf("dirs=%d_files=4", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				entries, total := ScanEntries(root)
				require.Len(b, entries, count)
				require.Positive(b, total)
			}
		})
	}
}

func benchmarkEntries(count int, now time.Time) []Entry {
	entries := make([]Entry, 0, count)
	for i := range count {
		entries = append(entries, Entry{
			KBID:      fmt.Sprintf("kb-%06d", i),
			Path:      fmt.Sprintf("/cache/kb-%06d", i),
			Bytes:     1024,
			LastTouch: now.Add(-time.Duration(i) * time.Second),
		})
	}
	return entries
}

func seedCacheDirs(b *testing.B, root string, dirs int, filesPerDir int) {
	b.Helper()
	payload := []byte("0123456789abcdef")
	for i := range dirs {
		dir := filepath.Join(root, fmt.Sprintf("kb-%06d", i))
		require.NoError(b, os.MkdirAll(dir, 0o755))
		for j := range filesPerDir {
			require.NoError(b, os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%02d", j)), payload, 0o644))
		}
	}
}
