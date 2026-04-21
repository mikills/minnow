package duckdb

import (
	"math"
	"sort"
	"time"

	kb "github.com/mikills/minnow/kb"
)

const maxCompactionCandidates = 4

func buildCompactedManifest(kbID string, current *kb.SnapshotShardManifest, replaced []kb.SnapshotShardMetadata, replacement kb.SnapshotShardMetadata) kb.SnapshotShardManifest {
	replacedByID := make(map[string]struct{}, len(replaced))
	for _, shard := range replaced {
		replacedByID[shard.ShardID] = struct{}{}
	}

	nextShards := make([]kb.SnapshotShardMetadata, 0, len(current.Shards)-len(replaced)+1)
	inserted := false
	for _, shard := range current.Shards {
		if _, drop := replacedByID[shard.ShardID]; drop {
			if !inserted {
				nextShards = append(nextShards, replacement)
				inserted = true
			}
			continue
		}
		nextShards = append(nextShards, shard)
	}
	if !inserted {
		nextShards = append(nextShards, replacement)
	}

	total := int64(0)
	for _, shard := range nextShards {
		total += shard.SizeBytes
	}

	return kb.SnapshotShardManifest{
		SchemaVersion:  current.SchemaVersion,
		Layout:         current.Layout,
		FormatKind:     current.FormatKind,
		FormatVersion:  current.FormatVersion,
		KBID:           kbID,
		CreatedAt:      time.Now().UTC(),
		TotalSizeBytes: total,
		Shards:         nextShards,
	}
}

func selectCompactionCandidatesWithReason(policy kb.ShardingPolicy, manifest *kb.SnapshotShardManifest) ([]kb.SnapshotShardMetadata, string) {
	policy = kb.NormalizeShardingPolicy(policy)
	if !shouldCompact(policy, manifest) {
		return nil, "no_compaction_debt"
	}

	tiers := make(map[int][]kb.SnapshotShardMetadata)
	for _, shard := range manifest.Shards {
		if shard.SizeBytes <= 0 {
			continue
		}
		tier := shardSizeTier(shard.SizeBytes, policy.TargetShardBytes)
		tiers[tier] = append(tiers[tier], shard)
	}

	if tier, ok := densestTier(tiers); ok && len(tiers[tier]) >= 2 {
		return sortAndLimitCompactionCandidates(tiers[tier], maxCompactionCandidates), "size_tier"
	}

	pressure := make([]kb.SnapshotShardMetadata, 0, len(manifest.Shards))
	for _, shard := range manifest.Shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			pressure = append(pressure, shard)
		}
	}
	if len(pressure) < 2 {
		return nil, "insufficient_candidates"
	}

	return sortAndLimitCompactionCandidates(pressure, maxCompactionCandidates), "tombstone_pressure"
}

func shouldCompact(policy kb.ShardingPolicy, manifest *kb.SnapshotShardManifest) bool {
	if !policy.CompactionEnabled || manifest == nil || len(manifest.Shards) < 2 {
		return false
	}
	if len(manifest.Shards) >= policy.CompactionMinShardCount {
		return true
	}
	for _, shard := range manifest.Shards {
		if shard.TombstoneRatio >= policy.CompactionTombstoneRatio {
			return true
		}
	}
	return false
}

func shardSizeTier(sizeBytes, targetBytes int64) int {
	if sizeBytes <= 0 || targetBytes <= 0 {
		return 0
	}
	ratio := float64(sizeBytes) / float64(targetBytes)
	return int(math.Round(math.Log2(ratio)))
}

func densestTier(tiers map[int][]kb.SnapshotShardMetadata) (int, bool) {
	bestTier := 0
	bestCount := 0
	found := false
	for tier, shards := range tiers {
		count := len(shards)
		if !found || count > bestCount || (count == bestCount && absInt(tier) < absInt(bestTier)) || (count == bestCount && absInt(tier) == absInt(bestTier) && tier < bestTier) {
			bestTier = tier
			bestCount = count
			found = true
		}
	}
	return bestTier, found
}

func sortAndLimitCompactionCandidates(candidates []kb.SnapshotShardMetadata, limit int) []kb.SnapshotShardMetadata {
	sorted := append([]kb.SnapshotShardMetadata(nil), candidates...)
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
