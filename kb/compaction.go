// Compaction merges multiple shard snapshot files (DuckDB shard parts) into a
// single compacted shard and publishes a new shard manifest that replaces the
// original shards. The goals are:
//
//   - Reduce shard count and size variance (consolidate fragmented shards).
//   - Reclaim storage from tombstoned/deleted rows.
//   - Rebuild the HNSW vector index for the combined data.
//
// System fit:
//
//   - Compaction acquires a write lease before performing any work, ensuring
//     only one compaction/ingest operation runs per KB at a time cluster-wide.
//   - Publishing the new manifest uses compare-and-set (UploadIfMatch) so
//     concurrent manifest changes are detected and rejected.
//   - Replaced shards are enqueued for garbage collection rather than deleted
//     immediately, allowing in-flight readers to finish gracefully.
//
// Selection algorithm:
//
//   - Shards are grouped into size tiers using log2(size / targetSize).
//   - The densest tier (most shards) is selected; up to maxCompactionCandidates
//     shards from that tier are merged per pass.
//   - Alternatively, shards with high tombstone ratios can trigger compaction
//     even if the tier density is low ("tombstone pressure").
//
// Failure modes:
//
//   - Network/blob errors abort compaction and return an error.
//   - Manifest CAS conflicts (ErrBlobVersionMismatch) abort compaction; the
//     caller can retry on a subsequent pass.
//   - Compaction is I/O and CPU intensive; temp files and HNSW index rebuilds
//     can be expensive for large shards.

package kb

import (
	"math"
	"sort"
	"time"
)

// maxCompactionCandidates is the upper bound on shards merged in one pass.
// Limiting candidates bounds memory/CPU usage per compaction cycle.
const maxCompactionCandidates = 4

// CompactionPublishResult describes the outcome of a single compaction attempt.
//
// When Performed is false, no compaction was necessary (insufficient candidates
// or compaction debt). When Performed is true, ReplacedShards were merged into
// ReplacementShards and the manifest was updated from ManifestVersionOld to
// ManifestVersionNew.
type CompactionPublishResult struct {
	Performed          bool
	ReplacedShards     []SnapshotShardMetadata
	ReplacementShards  []SnapshotShardMetadata
	ManifestVersionOld string
	ManifestVersionNew string
}

// buildCompactedManifest produces a new manifest with the replaced shards
// removed and the replacement shard inserted in their place.
//
// The replacement is inserted at the position of the first replaced shard to
// preserve approximate ordering. TotalSizeBytes is recalculated.
func buildCompactedManifest(kbID string, current *SnapshotShardManifest, replaced []SnapshotShardMetadata, replacement SnapshotShardMetadata) SnapshotShardManifest {
	replacedByID := make(map[string]struct{}, len(replaced))
	for _, shard := range replaced {
		replacedByID[shard.ShardID] = struct{}{}
	}

	nextShards := make([]SnapshotShardMetadata, 0, len(current.Shards)-len(replaced)+1)
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

	return SnapshotShardManifest{
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

// selectCompactionCandidatesWithReason evaluates the manifest and returns a
// deterministic candidate set for compaction along with the selection reason.
//
// Selection logic:
//  1. If shouldCompact returns false, returns nil with reason "no_compaction_debt".
//  2. Group shards by size tier (log2(size / targetSize)).
//  3. Select the densest tier (most shards). If it has >= 2 shards, return up
//     to maxCompactionCandidates from that tier (reason "size_tier").
//  4. Otherwise, collect shards with TombstoneRatio >= threshold. If >= 2,
//     return up to maxCompactionCandidates (reason "tombstone_pressure").
//  5. If neither condition yields >= 2 candidates, return nil with reason
//     "insufficient_candidates".
//
// Candidates are sorted by tombstone ratio (desc), size (asc), shard ID (asc)
// for deterministic selection.
func selectCompactionCandidatesWithReason(policy ShardingPolicy, manifest *SnapshotShardManifest) ([]SnapshotShardMetadata, string) {
	policy = NormalizeShardingPolicy(policy)
	if !shouldCompact(policy, manifest) {
		return nil, "no_compaction_debt"
	}

	tiers := make(map[int][]SnapshotShardMetadata)
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

	pressure := make([]SnapshotShardMetadata, 0, len(manifest.Shards))
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

// selectCompactionCandidates is a convenience function that calls
// selectCompactionCandidatesWithReason and discards the reason.
func selectCompactionCandidates(policy ShardingPolicy, manifest *SnapshotShardManifest) []SnapshotShardMetadata {
	candidates, _ := selectCompactionCandidatesWithReason(policy, manifest)
	return candidates
}

// shouldCompact returns true when compaction is enabled and the manifest has
// compaction debt: either enough shards to trigger size-tiered compaction or
// at least one shard with excessive tombstone ratio.
func shouldCompact(policy ShardingPolicy, manifest *SnapshotShardManifest) bool {
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

// shardSizeTier computes the size tier for a shard as round(log2(size / target)).
// Shards within the same tier are considered similar in size and good candidates
// for merging together.
func shardSizeTier(sizeBytes, targetBytes int64) int {
	if sizeBytes <= 0 || targetBytes <= 0 {
		return 0
	}
	ratio := float64(sizeBytes) / float64(targetBytes)
	return int(math.Round(math.Log2(ratio)))
}

// densestTier returns the tier with the most shards. When multiple tiers tie,
// it prefers the tier closest to 0 (target size), then the lower tier number.
func densestTier(tiers map[int][]SnapshotShardMetadata) (int, bool) {
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

// sortAndLimitCompactionCandidates sorts candidates by tombstone ratio (desc),
// size (asc), and shard ID (asc) for deterministic selection, then limits to
// the specified count.
func sortAndLimitCompactionCandidates(candidates []SnapshotShardMetadata, limit int) []SnapshotShardMetadata {
	sorted := append([]SnapshotShardMetadata(nil), candidates...)
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

// absInt returns the absolute value of an integer.
func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
