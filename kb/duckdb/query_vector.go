package duckdb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	kb "github.com/mikills/minnow/kb"
)

type vectorQueryPath int

const (
	vectorQueryPathSmallShardFast vectorQueryPath = iota
	vectorQueryPathShardFanout
)

type shardQueryPlan struct {
	Shards      []kb.SnapshotShardMetadata
	Fanout      int
	Parallelism int
	Capped      bool
}

type vectorQuerySelection struct {
	Path vectorQueryPath
	Plan shardQueryPlan
}

func (f *DuckDBArtifactFormat) planShardFanout(policy kb.ShardingPolicy, manifest *kb.SnapshotShardManifest, queryVec []float32) shardQueryPlan {
	if manifest == nil || len(manifest.Shards) == 0 {
		return shardQueryPlan{}
	}

	ranked := rankShardsForQuery(manifest.Shards, queryVec)

	baseFanout := policy.QueryShardFanout
	if baseFanout <= 0 {
		baseFanout = 1
	}
	adaptiveMax := policy.QueryShardFanoutAdaptiveMax
	if adaptiveMax <= 0 {
		adaptiveMax = 1
	}
	parallelism := policy.QueryShardParallelism
	if parallelism <= 0 {
		parallelism = 1
	}

	fanout := baseFanout
	if fanout > adaptiveMax {
		fanout = adaptiveMax
	}
	if fanout > len(ranked) {
		fanout = len(ranked)
	}
	if fanout <= 0 {
		return shardQueryPlan{}
	}
	if parallelism > fanout {
		parallelism = fanout
	}

	selected := make([]kb.SnapshotShardMetadata, fanout)
	copy(selected, ranked[:fanout])

	return shardQueryPlan{
		Shards:      selected,
		Fanout:      fanout,
		Parallelism: parallelism,
		Capped:      baseFanout > adaptiveMax,
	}
}

type scoredShard struct {
	shard kb.SnapshotShardMetadata
	score float64
}

func rankShardsForQuery(shards []kb.SnapshotShardMetadata, queryVec []float32) []kb.SnapshotShardMetadata {
	scored := make([]scoredShard, len(shards))
	for i := range shards {
		scored[i] = scoredShard{shard: shards[i], score: shardRankScore(shards[i], queryVec)}
	}

	sort.SliceStable(scored, func(i, j int) bool {
		si, sj := scored[i], scored[j]
		if si.score != sj.score {
			return si.score < sj.score
		}
		if si.shard.VectorRows != sj.shard.VectorRows {
			return si.shard.VectorRows > sj.shard.VectorRows
		}
		if si.shard.ShardID != sj.shard.ShardID {
			return si.shard.ShardID < sj.shard.ShardID
		}
		return si.shard.Key < sj.shard.Key
	})

	ranked := make([]kb.SnapshotShardMetadata, len(scored))
	for i := range scored {
		ranked[i] = scored[i].shard
	}
	return ranked
}

func shardRankScore(shard kb.SnapshotShardMetadata, queryVec []float32) float64 {
	if len(queryVec) > 0 && len(shard.Centroid) == len(queryVec) {
		total := 0.0
		for i := range queryVec {
			delta := float64(shard.Centroid[i] - queryVec[i])
			total += delta * delta
		}
		return total
	}

	if shard.VectorRows > 0 {
		return -float64(shard.VectorRows)
	}

	return 0
}

func (f *DuckDBArtifactFormat) selectVectorQueryPath(ctx context.Context, kbID string) (vectorQueryPath, error) {
	selection, err := f.resolveVectorQuerySelection(ctx, kbID, nil)
	if err != nil {
		return vectorQueryPathShardFanout, err
	}
	return selection.Path, nil
}

func (f *DuckDBArtifactFormat) resolveVectorQuerySelection(ctx context.Context, kbID string, queryVec []float32) (*vectorQuerySelection, error) {
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
		parallelism := policy.QueryShardParallelism
		if parallelism <= 0 {
			parallelism = 1
		}
		if parallelism > len(manifest.Shards) {
			parallelism = len(manifest.Shards)
		}
		shards := make([]kb.SnapshotShardMetadata, len(manifest.Shards))
		copy(shards, manifest.Shards)
		return &vectorQuerySelection{
			Path: vectorQueryPathSmallShardFast,
			Plan: shardQueryPlan{
				Shards:      shards,
				Fanout:      len(shards),
				Parallelism: parallelism,
			},
		}, nil
	}

	plan := f.planShardFanout(policy, manifest, queryVec)
	if plan.Fanout <= 0 {
		return nil, kb.ErrKBUninitialized
	}
	return &vectorQuerySelection{Path: vectorQueryPathShardFanout, Plan: plan}, nil
}

func (f *DuckDBArtifactFormat) searchTopK(ctx context.Context, kbID string, queryVec []float32, k int) ([]kb.QueryResult, error) {
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
	localTopK := shardLocalTopK(k, policy)
	results, shardErr := f.queryTopKFromShards(ctx, kbID, queryVec, k, localTopK, selection.Plan)
	if shardErr != nil {
		f.deps.Metrics.RecordShardExecutionFailure(kbID)
		return nil, shardErr
	}
	return results, nil
}

func shardLocalTopK(k int, policy kb.ShardingPolicy) int {
	if k <= 0 {
		return 0
	}
	mult := policy.QueryShardLocalTopKMult
	if mult <= 0 {
		mult = 1
	}
	local := k * mult
	if local < k {
		return k
	}
	return local
}

func (f *DuckDBArtifactFormat) queryTopKFromShards(ctx context.Context, kbID string, queryVec []float32, k, localTopK int, plan shardQueryPlan) ([]kb.QueryResult, error) {
	if plan.Fanout <= 0 || len(plan.Shards) == 0 {
		return []kb.QueryResult{}, nil
	}
	f.deps.Metrics.RecordShardFanout(kbID, plan.Fanout, plan.Capped)
	f.deps.Metrics.RecordShardExecution(kbID, plan.Fanout)

	shardResults, err := f.queryShardsTopK(ctx, kbID, plan.Shards, queryVec, localTopK, plan.Parallelism)
	if err != nil {
		return nil, err
	}
	return mergeShardTopKResults(shardResults, k), nil
}

func (f *DuckDBArtifactFormat) queryShardsTopK(ctx context.Context, kbID string, shards []kb.SnapshotShardMetadata, queryVec []float32, k, parallelism int) ([][]kb.QueryResult, error) {
	if len(shards) == 0 || k <= 0 {
		return [][]kb.QueryResult{}, nil
	}
	if parallelism <= 0 {
		parallelism = 1
	}
	if parallelism > len(shards) {
		parallelism = len(shards)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]kb.QueryResult, len(shards))
	sem := make(chan struct{}, parallelism)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := range shards {
		idx := i
		shard := shards[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			rows, err := f.querySingleShardTopK(ctx, kbID, shard, queryVec, k)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			results[idx] = rows
		}()
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	return results, nil
}

func (f *DuckDBArtifactFormat) querySingleShardTopK(ctx context.Context, kbID string, shard kb.SnapshotShardMetadata, queryVec []float32, k int) ([]kb.QueryResult, error) {
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

func (f *DuckDBArtifactFormat) ensureLocalShardFile(ctx context.Context, kbID string, shard kb.SnapshotShardMetadata) (string, bool, error) {
	if strings.TrimSpace(shard.Key) == "" {
		return "", false, fmt.Errorf("shard key is required")
	}
	cacheDir := filepath.Join(f.deps.CacheDir, kbID, "query-shards")
	localPath := filepath.Join(cacheDir, shardCacheFileName(shard))
	if _, err := os.Stat(localPath); err == nil {
		if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
			return "", true, err
		}
		return localPath, true, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}

	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", false, err
	}

	tmpPath := fmt.Sprintf("%s.download-%d", localPath, time.Now().UnixNano())
	defer os.Remove(tmpPath)
	if err := f.deps.BlobStore.Download(ctx, shard.Key, tmpPath); err != nil {
		return "", false, err
	}

	if shard.SizeBytes > 0 {
		stat, err := os.Stat(tmpPath)
		if err != nil {
			return "", false, err
		}
		if stat.Size() != shard.SizeBytes {
			return "", false, fmt.Errorf("size mismatch for shard %s", shard.ShardID)
		}
	}
	if shard.SHA256 != "" {
		sha, err := kb.FileContentSHA256(tmpPath)
		if err != nil {
			return "", false, err
		}
		if sha != shard.SHA256 {
			return "", false, fmt.Errorf("checksum mismatch for shard %s", shard.ShardID)
		}
	}
	if err := os.Rename(tmpPath, localPath); err != nil {
		return "", false, err
	}
	if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
		return "", false, err
	}
	return localPath, false, nil
}

func shardCacheFileName(shard kb.SnapshotShardMetadata) string {
	token := shard.ShardID
	if strings.TrimSpace(token) == "" {
		token = "shard"
	}
	cleanToken := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_':
			return r
		default:
			return '_'
		}
	}, token)
	if cleanToken == "" {
		cleanToken = "shard"
	}
	digest := sha256.Sum256([]byte(shard.Key + "|" + shard.Version))
	return fmt.Sprintf("%s-%x.duckdb", cleanToken, digest[:8])
}

func mergeShardTopKResults(shardResults [][]kb.QueryResult, k int) []kb.QueryResult {
	if k <= 0 || len(shardResults) == 0 {
		return []kb.QueryResult{}
	}

	type scoredResult struct {
		result     kb.QueryResult
		shardIndex int
		localIndex int
	}

	total := 0
	for _, shard := range shardResults {
		total += len(shard)
	}

	flattened := make([]scoredResult, 0, total)
	for shardIndex, shard := range shardResults {
		for localIndex, result := range shard {
			flattened = append(flattened, scoredResult{
				result:     result,
				shardIndex: shardIndex,
				localIndex: localIndex,
			})
		}
	}

	if len(flattened) == 0 {
		return []kb.QueryResult{}
	}

	sort.SliceStable(flattened, func(i, j int) bool {
		left := flattened[i]
		right := flattened[j]

		if left.result.Distance != right.result.Distance {
			return left.result.Distance < right.result.Distance
		}
		if left.result.ID != right.result.ID {
			return left.result.ID < right.result.ID
		}
		if left.result.Content != right.result.Content {
			return left.result.Content < right.result.Content
		}
		if left.shardIndex != right.shardIndex {
			return left.shardIndex < right.shardIndex
		}

		return left.localIndex < right.localIndex
	})

	if k > len(flattened) {
		k = len(flattened)
	}

	merged := make([]kb.QueryResult, 0, k)
	for i := 0; i < k; i++ {
		merged = append(merged, flattened[i].result)
	}

	return merged
}
