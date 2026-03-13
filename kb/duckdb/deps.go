package duckdb

import (
	"context"

	kb "github.com/mikills/kbcore/kb"
)

// DepOption configures DuckDB-specific fields on DuckDBArtifactDeps.
type DepOption func(*DuckDBArtifactDeps)

// WithMemoryLimit sets the DuckDB per-connection memory limit (e.g. "128MB").
func WithMemoryLimit(limit string) DepOption {
	return func(d *DuckDBArtifactDeps) { d.MemoryLimit = limit }
}

// WithExtensionDir sets the DuckDB extension directory path.
func WithExtensionDir(dir string) DepOption {
	return func(d *DuckDBArtifactDeps) { d.ExtensionDir = dir }
}

// WithOfflineExt controls whether DuckDB extensions are loaded offline.
func WithOfflineExt(offline bool) DepOption {
	return func(d *DuckDBArtifactDeps) { d.OfflineExt = offline }
}

// NewDepsFromKB constructs DuckDBArtifactDeps by wiring common fields from a
// *kb.KB instance. DuckDB-specific settings (memory limit, extension dir, etc.)
// are applied via functional options.
func NewDepsFromKB(k *kb.KB, opts ...DepOption) DuckDBArtifactDeps {
	deps := DuckDBArtifactDeps{
		BlobStore:      k.BlobStore,
		ManifestStore:  k.ManifestStore,
		CacheDir:       k.CacheDir,
		ExtensionDir:   ResolveExtensionDir(),
		OfflineExt:     true,
		ShardingPolicy: k.ShardingPolicy,
		Embed:          k.Embed,
		GraphBuilder:   func() *kb.GraphBuilder { return k.GraphBuilder },
		EvictCacheIfNeeded: func(ctx context.Context, protectKBID string) error {
			return k.EvictCacheIfNeeded(ctx, protectKBID)
		},
		LockFor: k.LockFor,
		AcquireWriteLease: func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error) {
			return k.AcquireWriteLease(ctx, kbID)
		},
		EnqueueReplacedShardsForGC: k.EnqueueReplacedShardsForGC,
		Metrics:                    k,
	}
	for _, opt := range opts {
		opt(&deps)
	}
	return deps
}
