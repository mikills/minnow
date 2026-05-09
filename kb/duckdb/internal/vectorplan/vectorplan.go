package vectorplan

import (
	"sort"

	kb "github.com/mikills/minnow/kb"
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
	shard *kb.SnapshotShardMetadata
	score float64
}

func RankShards(shards []kb.SnapshotShardMetadata, queryVec []float32) []kb.SnapshotShardMetadata {
	scored := make([]scoredShard, len(shards))
	for i := range shards {
		scored[i] = scoredShard{shard: &shards[i], score: shardRankScore(shards[i], queryVec)}
	}
	sort.SliceStable(scored, func(i, j int) bool { return lessScoredShard(scored[i], scored[j]) })
	ranked := make([]kb.SnapshotShardMetadata, len(scored))
	for i := range scored {
		ranked[i] = *scored[i].shard
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
	total := countShardResults(shardResults)
	if total == 0 {
		return []kb.QueryResult{}
	}
	if k > total {
		k = total
	}
	if shouldSortAllTopK(k, total) {
		return mergeTopKByFullSort(shardResults, k)
	}
	best := topKResultHeap{items: make([]scoredResult, 0, k)}
	for shardIndex, shard := range shardResults {
		for localIndex, result := range shard {
			best.add(scoredResult{result: result, shardIndex: shardIndex, localIndex: localIndex}, k)
		}
	}
	sort.SliceStable(best.items, func(i, j int) bool { return lessScoredResult(best.items[i], best.items[j]) })
	merged := make([]kb.QueryResult, len(best.items))
	for i := range best.items {
		merged[i] = best.items[i].result
	}
	return merged
}

func shouldSortAllTopK(k int, total int) bool {
	return k*4 >= total
}

func mergeTopKByFullSort(shardResults [][]kb.QueryResult, k int) []kb.QueryResult {
	refs := flattenShardResultRefs(shardResults)
	sort.SliceStable(refs, func(i, j int) bool { return lessResultRef(shardResults, refs[i], refs[j]) })
	merged := make([]kb.QueryResult, k)
	for i := range merged {
		ref := refs[i]
		merged[i] = shardResults[ref.shardIndex][ref.localIndex]
	}
	return merged
}

type scoredResult struct {
	result     kb.QueryResult
	shardIndex int
	localIndex int
}

type topKResultHeap struct{ items []scoredResult }

func (h *topKResultHeap) add(candidate scoredResult, k int) {
	if len(h.items) < k {
		h.items = append(h.items, candidate)
		h.siftUp(len(h.items) - 1)
		return
	}
	if lessScoredResult(candidate, h.items[0]) {
		h.items[0] = candidate
		h.siftDown(0)
	}
}

func (h *topKResultHeap) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !worseScoredResult(h.items[index], h.items[parent]) {
			return
		}
		h.items[index], h.items[parent] = h.items[parent], h.items[index]
		index = parent
	}
}

func (h *topKResultHeap) siftDown(index int) {
	for {
		left := index*2 + 1
		if left >= len(h.items) {
			return
		}
		child := left
		right := left + 1
		if right < len(h.items) && worseScoredResult(h.items[right], h.items[left]) {
			child = right
		}
		if !worseScoredResult(h.items[child], h.items[index]) {
			return
		}
		h.items[index], h.items[child] = h.items[child], h.items[index]
		index = child
	}
}

func worseScoredResult(left scoredResult, right scoredResult) bool {
	return lessScoredResult(right, left)
}

type resultRef struct {
	shardIndex int
	localIndex int
}

func flattenShardResultRefs(shardResults [][]kb.QueryResult) []resultRef {
	refs := make([]resultRef, 0, countShardResults(shardResults))
	for shardIndex, shard := range shardResults {
		for localIndex := range shard {
			refs = append(refs, resultRef{shardIndex: shardIndex, localIndex: localIndex})
		}
	}
	return refs
}

func lessResultRef(shardResults [][]kb.QueryResult, left resultRef, right resultRef) bool {
	return lessScoredResult(
		scoredResult{
			result:     shardResults[left.shardIndex][left.localIndex],
			shardIndex: left.shardIndex,
			localIndex: left.localIndex,
		},
		scoredResult{
			result:     shardResults[right.shardIndex][right.localIndex],
			shardIndex: right.shardIndex,
			localIndex: right.localIndex,
		},
	)
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
