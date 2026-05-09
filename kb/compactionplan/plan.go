package compactionplan

import (
	"math"
	"sort"

	"github.com/mikills/minnow/kb/sharding"
)

const MaxCandidates = 1 << 2

type Shard struct {
	ShardID        string
	SizeBytes      int64
	TombstoneRatio float64
}

func SelectWithReason(policy sharding.Policy, shards []Shard) ([]Shard, string) {
	policy = sharding.NormalizePolicy(policy)
	if !ShouldCompact(policy, shards) {
		return nil, "no_compaction_debt"
	}
	tiers := make(map[int][]Shard)
	for _, shard := range shards {
		if shard.SizeBytes <= 0 {
			continue
		}
		tier := SizeTier(shard.SizeBytes, policy.TargetShardBytes)
		tiers[tier] = append(tiers[tier], shard)
	}
	if tier, ok := densestTier(tiers); ok && len(tiers[tier]) >= 2 {
		return SortAndLimit(tiers[tier], MaxCandidates), "size_tier"
	}
	pressure := make([]Shard, 0, len(shards))
	for _, shard := range shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			pressure = append(pressure, shard)
		}
	}
	if len(pressure) < 2 {
		return nil, "insufficient_candidates"
	}
	return SortAndLimit(pressure, MaxCandidates), "tombstone_pressure"
}

func Select(policy sharding.Policy, shards []Shard) []Shard {
	candidates, _ := SelectWithReason(policy, shards)
	return candidates
}

func ShouldCompact(policy sharding.Policy, shards []Shard) bool {
	if !policy.CompactionEnabled || len(shards) < 2 {
		return false
	}
	if len(shards) >= policy.CompactionMinShardCount {
		return true
	}
	for _, shard := range shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			return true
		}
	}
	return false
}

func SizeTier(sizeBytes, targetBytes int64) int {
	if sizeBytes <= 0 || targetBytes <= 0 {
		return 0
	}
	ratio := float64(sizeBytes) / float64(targetBytes)
	return int(math.Round(math.Log2(ratio)))
}

func densestTier(tiers map[int][]Shard) (int, bool) {
	bestTier := 0
	bestCount := 0
	found := false
	for tier, shards := range tiers {
		count := len(shards)
		if !found || count > bestCount || count == bestCount && absInt(tier) < absInt(bestTier) ||
			count == bestCount && absInt(tier) == absInt(bestTier) && tier < bestTier {
			bestTier = tier
			bestCount = count
			found = true
		}
	}
	return bestTier, found
}

func SortAndLimit(candidates []Shard, limit int) []Shard {
	sorted := append([]Shard(nil), candidates...)
	sort.Slice(sorted, func(i, j int) bool {
		left := sorted[i]
		right := sorted[j]
		if left.TombstoneRatio != right.TombstoneRatio {
			return left.TombstoneRatio > right.TombstoneRatio
		}
		if left.SizeBytes != right.SizeBytes {
			return left.SizeBytes < right.SizeBytes
		}
		return left.ShardID < right.ShardID
	})
	if limit > 0 && len(sorted) > limit {
		sorted = sorted[:limit]
	}
	return sorted
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
