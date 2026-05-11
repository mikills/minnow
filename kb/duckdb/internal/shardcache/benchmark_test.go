package shardcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/cacheevict"
	"github.com/stretchr/testify/require"
)

func BenchmarkEnsureLocalFile(b *testing.B) {
	ctx := context.Background()
	for _, size := range []int{64 << 10, 4 << 20} {
		b.Run(fmt.Sprintf("cold_size=%d", size), func(b *testing.B) {
			store, shard := seedBenchShard(b, size, "v1")
			b.ReportAllocs()
			for b.Loop() {
				cacheDir := b.TempDir()
				manager := Manager{
					CacheDir:           cacheDir,
					BlobStore:          store,
					EvictCacheIfNeeded: func(context.Context, string) error { return nil },
				}
				_, cached, err := manager.EnsureLocalFile(ctx, "kb", shard)
				require.NoError(b, err)
				require.False(b, cached)
			}
		})
		b.Run(fmt.Sprintf("warm_size=%d", size), func(b *testing.B) {
			store, shard := seedBenchShard(b, size, "v1")
			cacheDir := b.TempDir()
			manager := Manager{
				CacheDir:           cacheDir,
				BlobStore:          store,
				EvictCacheIfNeeded: func(context.Context, string) error { return nil },
			}
			_, cached, err := manager.EnsureLocalFile(ctx, "kb", shard)
			require.NoError(b, err)
			require.False(b, cached)
			b.ReportAllocs()
			for b.Loop() {
				_, cached, err := manager.EnsureLocalFile(ctx, "kb", shard)
				require.NoError(b, err)
				require.True(b, cached)
			}
		})
	}
}

func BenchmarkEnsureLocalFileWarmWithEvictionSweep(b *testing.B) {
	ctx := context.Background()
	store, shard := seedBenchShard(b, 64<<10, "v1")
	for _, cachedKBs := range []int{100, 1000} {
		b.Run(fmt.Sprintf("cached_kbs=%d", cachedKBs), func(b *testing.B) {
			cacheDir := b.TempDir()
			seedCacheDirs(b, cacheDir, cachedKBs, 2)
			manager := Manager{
				CacheDir:  cacheDir,
				BlobStore: store,
				EvictCacheIfNeeded: func(_ context.Context, protectKBID string) error {
					_ = cacheevict.Sweep(cacheevict.Config{
						Root:      cacheDir,
						MaxBytes:  int64(cachedKBs+1) * 1024 * 1024,
						Protected: map[string]bool{protectKBID: true},
						Remove:    func(cacheevict.Entry, cacheevict.Reason) bool { return true },
					})
					return nil
				},
			}
			_, cached, err := manager.EnsureLocalFile(ctx, "kb", shard)
			require.NoError(b, err)
			require.False(b, cached)
			b.ReportAllocs()
			for b.Loop() {
				_, cached, err := manager.EnsureLocalFile(ctx, "kb", shard)
				require.NoError(b, err)
				require.True(b, cached)
			}
		})
	}
}

func BenchmarkEnsureLocalFileVersionChange(b *testing.B) {
	ctx := context.Background()
	store, first := seedBenchShard(b, 64<<10, "v1")
	_, second := seedBenchShardInStore(b, store, 64<<10, "v2")
	cacheDir := b.TempDir()
	manager := Manager{
		CacheDir:           cacheDir,
		BlobStore:          store,
		EvictCacheIfNeeded: func(context.Context, string) error { return nil },
	}
	_, cached, err := manager.EnsureLocalFile(ctx, "kb", first)
	require.NoError(b, err)
	require.False(b, cached)
	b.ReportAllocs()
	for b.Loop() {
		_, _, err := manager.EnsureLocalFile(ctx, "kb", second)
		require.NoError(b, err)
	}
}

type benchShardBlobStore struct{ root string }

func (s benchShardBlobStore) Download(_ context.Context, key string, dest string) error {
	data, err := os.ReadFile(filepath.Join(s.root, key))
	if err != nil {
		return err
	}
	return os.WriteFile(dest, data, 0o644)
}

func seedCacheDirs(b *testing.B, root string, dirs int, filesPerDir int) {
	b.Helper()
	payload := []byte("0123456789abcdef")
	for i := range dirs {
		dir := filepath.Join(root, fmt.Sprintf("cached-kb-%06d", i))
		require.NoError(b, os.MkdirAll(dir, 0o755))
		for j := range filesPerDir {
			require.NoError(b, os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%02d", j)), payload, 0o644))
		}
	}
}

func seedBenchShard(b *testing.B, size int, version string) (benchShardBlobStore, kb.SnapshotShardMetadata) {
	store := benchShardBlobStore{root: b.TempDir()}
	_, shard := seedBenchShardInStore(b, store, size, version)
	return store, shard
}

func seedBenchShardInStore(
	b *testing.B,
	store benchShardBlobStore,
	size int,
	version string,
) (string, kb.SnapshotShardMetadata) {
	b.Helper()
	key := fmt.Sprintf("shards/shard-%s.duckdb", version)
	path := filepath.Join(store.root, key)
	require.NoError(b, os.MkdirAll(filepath.Dir(path), 0o755))
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}
	require.NoError(b, os.WriteFile(path, data, 0o644))
	sum := sha256.Sum256(data)
	return path, kb.SnapshotShardMetadata{
		ShardID:   "shard",
		Key:       key,
		Version:   version,
		SizeBytes: int64(size),
		SHA256:    hex.EncodeToString(sum[:]),
	}
}
