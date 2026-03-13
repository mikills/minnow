package kb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompaction(t *testing.T) {
	t.Run("threshold_decision", testCompactionThresholdDecision)
	t.Run("select_candidates_size_tiered", testCompactionSelectCandidatesSizeTiered)
	t.Run("select_candidates_tombstone_pressure_fallback", testCompactionSelectCandidatesTombstonePressureFallback)
}

func testCompactionThresholdDecision(t *testing.T) {
	tests := []struct {
		name     string
		policy   ShardingPolicy
		manifest *SnapshotShardManifest
		want     bool
	}{
		{
			name: "disabled",
			policy: ShardingPolicy{
				CompactionEnabled: false,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1"}, {ShardID: "s2"}}},
			want:     false,
		},
		{
			name: "below_debt_thresholds",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  8,
				CompactionTombstoneRatio: 0.20,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1", TombstoneRatio: 0.10}, {ShardID: "s2", TombstoneRatio: 0.19}}},
			want:     false,
		},
		{
			name: "debt_from_shard_count",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  3,
				CompactionTombstoneRatio: 0.90,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1"}, {ShardID: "s2"}, {ShardID: "s3"}}},
			want:     true,
		},
		{
			name: "debt_from_tombstone_pressure",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  8,
				CompactionTombstoneRatio: 0.20,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1", TombstoneRatio: 0.25}, {ShardID: "s2", TombstoneRatio: 0.05}}},
			want:     true,
		},
		{
			name: "single_shard_never_compacts",
			policy: ShardingPolicy{
				CompactionEnabled:        true,
				CompactionMinShardCount:  1,
				CompactionTombstoneRatio: 0.01,
			},
			manifest: &SnapshotShardManifest{Shards: []SnapshotShardMetadata{{ShardID: "s1", TombstoneRatio: 0.8}}},
			want:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldCompact(tc.policy, tc.manifest))
		})
	}
}

func testCompactionSelectCandidatesSizeTiered(t *testing.T) {
	policy := ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  3,
		CompactionTombstoneRatio: 0.20,
		TargetShardBytes:         100,
	}

	manifest := &SnapshotShardManifest{Shards: []SnapshotShardMetadata{
		{ShardID: "s-small", SizeBytes: 20, TombstoneRatio: 0.30},
		{ShardID: "s-a", SizeBytes: 90, TombstoneRatio: 0.10},
		{ShardID: "s-b", SizeBytes: 95, TombstoneRatio: 0.40},
		{ShardID: "s-c", SizeBytes: 110, TombstoneRatio: 0.20},
		{ShardID: "s-large", SizeBytes: 220, TombstoneRatio: 0.90},
	}}

	candidates := selectCompactionCandidates(policy, manifest)
	require.Len(t, candidates, 3)
	assert.Equal(t, []string{"s-b", "s-c", "s-a"}, []string{candidates[0].ShardID, candidates[1].ShardID, candidates[2].ShardID})
}

func testCompactionSelectCandidatesTombstonePressureFallback(t *testing.T) {
	policy := ShardingPolicy{
		CompactionEnabled:        true,
		CompactionMinShardCount:  8,
		CompactionTombstoneRatio: 0.30,
		TargetShardBytes:         100,
	}

	manifest := &SnapshotShardManifest{Shards: []SnapshotShardMetadata{
		{ShardID: "s-1", SizeBytes: 40, TombstoneRatio: 0.35},
		{ShardID: "s-2", SizeBytes: 120, TombstoneRatio: 0.32},
		{ShardID: "s-3", SizeBytes: 260, TombstoneRatio: 0.10},
	}}

	candidates := selectCompactionCandidates(policy, manifest)
	require.Len(t, candidates, 2)
	assert.Equal(t, []string{"s-1", "s-2"}, []string{candidates[0].ShardID, candidates[1].ShardID})
}
