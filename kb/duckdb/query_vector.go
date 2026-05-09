package duckdb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/shardcache"
	"github.com/mikills/minnow/kb/duckdb/internal/vectorplan"
)

type vectorQueryPath int

const (
	vectorQueryPathSmallShardFast vectorQueryPath = iota
	vectorQueryPathShardFanout
)

type shardQueryPlan = vectorplan.QueryPlan

type vectorQuerySelection struct {
	Path vectorQueryPath
	Plan shardQueryPlan
}

func (f *DuckDBArtifactFormat) selectVectorQueryPath(ctx context.Context, kbID string) (vectorQueryPath, error) {
	selection, err := f.resolveVectorQuerySelection(ctx, kbID, nil)
	if err != nil {
		return vectorQueryPathShardFanout, err
	}
	return selection.Path, nil
}

func (f *DuckDBArtifactFormat) resolveVectorQuerySelection(
	ctx context.Context,
	kbID string,
	queryVec []float32,
) (*vectorQuerySelection, error) {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return nil, kb.ErrKBUninitialized
		}
		return nil, err
	}
	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return nil, err
	}
	f.deps.Metrics.RecordShardCount(kbID, len(manifest.Shards))
	if len(manifest.Shards) == 0 {
		return nil, kb.ErrKBUninitialized
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if len(manifest.Shards) <= policy.SmallKBMaxShards {
		return &vectorQuerySelection{
			Path: vectorQueryPathSmallShardFast,
			Plan: vectorplan.SmallShardPlan(policy, manifest.Shards),
		}, nil
	}

	plan := vectorplan.PlanShardFanout(policy, manifest, queryVec)
	if plan.Fanout <= 0 {
		return nil, kb.ErrKBUninitialized
	}
	return &vectorQuerySelection{Path: vectorQueryPathShardFanout, Plan: plan}, nil
}

func (f *DuckDBArtifactFormat) searchTopK(
	ctx context.Context,
	kbID string,
	queryVec []float32,
	k int,
) ([]kb.QueryResult, error) {
	if k <= 0 {
		return []kb.QueryResult{}, nil
	}
	if len(queryVec) == 0 {
		return nil, fmt.Errorf("query vector cannot be empty")
	}

	selection, err := f.resolveVectorQuerySelection(ctx, kbID, queryVec)
	if err != nil {
		return nil, fmt.Errorf("select vector query path: %w", err)
	}
	if err := validateQueryVectorDimensionForShards(queryVec, selection.Plan.Shards); err != nil {
		return nil, err
	}
	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	localTopK := vectorplan.LocalTopK(k, policy)
	results, shardErr := f.queryTopKFromShards(
		ctx,
		shardTopKQuery{kbID: kbID, queryVec: queryVec, k: k, localTopK: localTopK, plan: selection.Plan},
	)
	if shardErr != nil {
		f.deps.Metrics.RecordShardExecutionFailure(kbID)
		return nil, shardErr
	}
	return results, nil
}

type shardTopKQuery struct {
	kbID      string
	queryVec  []float32
	k         int
	localTopK int
	plan      shardQueryPlan
}

func (f *DuckDBArtifactFormat) queryTopKFromShards(
	ctx context.Context,
	query shardTopKQuery,
) ([]kb.QueryResult, error) {
	if query.plan.Fanout <= 0 || len(query.plan.Shards) == 0 {
		return []kb.QueryResult{}, nil
	}
	f.deps.Metrics.RecordShardFanout(query.kbID, query.plan.Fanout, query.plan.Capped)
	f.deps.Metrics.RecordShardExecution(query.kbID, query.plan.Fanout)

	shardResults, err := f.queryShardsTopK(
		ctx,
		shardQueryWorkload{
			kbID:        query.kbID,
			shards:      query.plan.Shards,
			queryVec:    query.queryVec,
			k:           query.localTopK,
			parallelism: query.plan.Parallelism,
		},
	)
	if err != nil {
		return nil, err
	}
	return vectorplan.MergeTopK(shardResults, query.k), nil
}

type shardQueryWorkload struct {
	kbID        string
	shards      []kb.SnapshotShardMetadata
	queryVec    []float32
	k           int
	parallelism int
}

func (f *DuckDBArtifactFormat) queryShardsTopK(
	ctx context.Context,
	workload shardQueryWorkload,
) ([][]kb.QueryResult, error) {
	if len(workload.shards) == 0 || workload.k <= 0 {
		return [][]kb.QueryResult{}, nil
	}
	parallelism := workload.parallelism
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > len(workload.shards) {
		parallelism = len(workload.shards)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]kb.QueryResult, len(workload.shards))
	sem := make(chan struct{}, parallelism)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := range workload.shards {
		f.startVectorShardQuery(
			ctx,
			vectorShardQueryWork{
				kbID:     workload.kbID,
				idx:      i,
				shard:    workload.shards[i],
				queryVec: workload.queryVec,
				k:        workload.k,
				sem:      sem,
				errCh:    errCh,
				results:  results,
				cancel:   cancel,
				wg:       &wg,
			},
		)
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	return results, nil
}

type vectorShardQueryWork struct {
	kbID     string
	idx      int
	shard    kb.SnapshotShardMetadata
	queryVec []float32
	k        int
	sem      chan struct{}
	errCh    chan error
	results  [][]kb.QueryResult
	cancel   context.CancelFunc
	wg       *sync.WaitGroup
}

func (f *DuckDBArtifactFormat) startVectorShardQuery(ctx context.Context, work vectorShardQueryWork) {
	work.wg.Add(1)
	go f.runVectorShardQuery(ctx, work)
}

func (f *DuckDBArtifactFormat) runVectorShardQuery(ctx context.Context, work vectorShardQueryWork) {
	defer work.wg.Done()
	select {
	case work.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	defer func() { <-work.sem }()
	rows, err := f.querySingleShardTopK(ctx, work.kbID, work.shard, work.queryVec, work.k)
	if err != nil {
		select {
		case work.errCh <- err:
		default:
		}
		work.cancel()
		return
	}
	work.results[work.idx] = rows
}

func (f *DuckDBArtifactFormat) querySingleShardTopK(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
	queryVec []float32,
	k int,
) ([]kb.QueryResult, error) {
	localPath, hit, err := f.ensureLocalShardFile(ctx, kbID, shard)
	if err != nil {
		return nil, fmt.Errorf("ensure shard file %s: %w", shard.ShardID, err)
	}
	f.deps.Metrics.RecordShardCacheAccess(kbID, hit)

	conn, err := f.pool.GetOrOpen(ctx, localPath, f.openConfiguredDB)
	if err != nil {
		return nil, fmt.Errorf("open shard %s: %w", shard.ShardID, err)
	}
	defer conn.mu.Unlock()

	results, err := QueryTopKWithDB(ctx, conn.db, queryVec, k)
	if err != nil {
		return nil, fmt.Errorf("query shard %s: %w", shard.ShardID, err)
	}
	return results, nil
}

func (f *DuckDBArtifactFormat) ensureLocalShardFile(
	ctx context.Context,
	kbID string,
	shard kb.SnapshotShardMetadata,
) (string, bool, error) {
	return shardcache.Manager{
		CacheDir:           f.deps.CacheDir,
		BlobStore:          f.deps.BlobStore,
		EvictCacheIfNeeded: f.deps.EvictCacheIfNeeded,
	}.EnsureLocalFile(ctx, kbID, shard)
}
