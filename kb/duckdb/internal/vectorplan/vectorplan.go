package vectorplan

import (
	kb "github.com/mikills/minnow/kb"
	"sort"
)

type QueryPlan struct {
	Shards      []kb.SnapshotShardMetadata
	Fanout      int
	Parallelism int
	Capped      bool
}

func PlanShardFanout(policy kb.ShardingPolicy, manifest *kb.SnapshotShardManifest, queryVec []float32) QueryPlan {
	if manifest == nil || len(manifest.Shards) == 0 {
		return QueryPlan{}
	}
	ranked := RankShards(manifest.Shards, queryVec)
	baseFanout := positiveOrOne(policy.QueryShardFanout)
	adaptiveMax := positiveOrOne(policy.QueryShardFanoutAdaptiveMax)
	parallelism := positiveOrOne(policy.QueryShardParallelism)
	fanout := minInt(baseFanout, adaptiveMax, len(ranked))
	if fanout <= 0 {
		return QueryPlan{}
	}
	parallelism = minInt(parallelism, fanout)
	selected := make([]kb.SnapshotShardMetadata, fanout)
	copy(selected, ranked[:fanout])
	return QueryPlan{Shards: selected, Fanout: fanout, Parallelism: parallelism, Capped: baseFanout > adaptiveMax}
}

func SmallShardPlan(policy kb.ShardingPolicy, shards []kb.SnapshotShardMetadata) QueryPlan {
	parallelism := positiveOrOne(policy.QueryShardParallelism)
	parallelism = minInt(parallelism, len(shards))
	selected := make([]kb.SnapshotShardMetadata, len(shards))
	copy(selected, shards)
	return QueryPlan{Shards: selected, Fanout: len(selected), Parallelism: parallelism}
}

func LocalTopK(k int, policy kb.ShardingPolicy) int {
	if k <= 0 {
		return 0
	}
	mult := positiveOrOne(policy.QueryShardLocalTopKMult)
	local := k * mult
	if local < k {
		return k
	}
	return local
}

type scoredShard struct {
	shard kb.SnapshotShardMetadata
	score float64
}

func RankShards(shards []kb.SnapshotShardMetadata, queryVec []float32) []kb.SnapshotShardMetadata {
	scored := make([]scoredShard, len(shards))
	for i := range shards {
		scored[i] = scoredShard{shard: shards[i], score: shardRankScore(shards[i], queryVec)}
	}
	sort.SliceStable(scored, func(i, j int) bool { return lessScoredShard(scored[i], scored[j]) })
	ranked := make([]kb.SnapshotShardMetadata, len(scored))
	for i := range scored {
		ranked[i] = scored[i].shard
	}
	return ranked
}

func lessScoredShard(left scoredShard, right scoredShard) bool {
	if left.score != right.score {
		return left.score < right.score
	}
	if left.shard.VectorRows != right.shard.VectorRows {
		return left.shard.VectorRows > right.shard.VectorRows
	}
	if left.shard.ShardID != right.shard.ShardID {
		return left.shard.ShardID < right.shard.ShardID
	}
	return left.shard.Key < right.shard.Key
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

func MergeTopK(shardResults [][]kb.QueryResult, k int) []kb.QueryResult {
	if k <= 0 || len(shardResults) == 0 {
		return []kb.QueryResult{}
	}
	flattened := flattenShardResults(shardResults)
	if len(flattened) == 0 {
		return []kb.QueryResult{}
	}
	sort.SliceStable(flattened, func(i, j int) bool { return lessScoredResult(flattened[i], flattened[j]) })
	if k > len(flattened) {
		k = len(flattened)
	}
	merged := make([]kb.QueryResult, 0, k)
	for i := 0; i < k; i++ {
		merged = append(merged, flattened[i].result)
	}
	return merged
}

type scoredResult struct {
	result     kb.QueryResult
	shardIndex int
	localIndex int
}

func flattenShardResults(shardResults [][]kb.QueryResult) []scoredResult {
	flattened := make([]scoredResult, 0, countShardResults(shardResults))
	for shardIndex, shard := range shardResults {
		for localIndex, result := range shard {
			flattened = append(flattened, scoredResult{result: result, shardIndex: shardIndex, localIndex: localIndex})
		}
	}
	return flattened
}

func countShardResults(shardResults [][]kb.QueryResult) int {
	total := 0
	for _, shard := range shardResults {
		total += len(shard)
	}
	return total
}

func lessScoredResult(left scoredResult, right scoredResult) bool {
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
}

func positiveOrOne(value int) int {
	if value <= 0 {
		return 1
	}
	return value
}

func minInt(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	m := values[0]
	for _, value := range values[1:] {
		if value < m {
			m = value
		}
	}
	return m
}
