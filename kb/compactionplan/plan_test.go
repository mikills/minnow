package compactionplan

import (
	"testing"

	"github.com/mikills/minnow/kb/sharding"
	"github.com/stretchr/testify/require"
)

func TestPlan(t *testing.T) {
	t.Run("selects largest compactable shards", func(t *testing.T) {
		policy := sharding.Policy{
			CompactionEnabled:        true,
			CompactionMinShardCount:  3,
			CompactionTombstoneRatio: 0.20,
			TargetShardBytes:         100,
		}
		shards := []Shard{
			{ShardID: "s-small", SizeBytes: 20, TombstoneRatio: 0.30},
			{ShardID: "s-a", SizeBytes: 90, TombstoneRatio: 0.10},
			{ShardID: "s-b", SizeBytes: 95, TombstoneRatio: 0.40},
			{ShardID: "s-c", SizeBytes: 110, TombstoneRatio: 0.20},
		}

		got := Select(policy, shards)

		require.Len(t, got, 3)
		require.Equal(t, []string{"s-b", "s-c", "s-a"}, []string{got[0].ShardID, got[1].ShardID, got[2].ShardID})
	})

	t.Run("tombstone pressure bypasses shard minimum", func(t *testing.T) {
		policy := sharding.Policy{
			CompactionEnabled:        true,
			CompactionMinShardCount:  8,
			CompactionTombstoneRatio: 0.30,
			TargetShardBytes:         100,
		}

		got := Select(policy, []Shard{
			{ShardID: "s-1", SizeBytes: 40, TombstoneRatio: 0.35},
			{ShardID: "s-2", SizeBytes: 120, TombstoneRatio: 0.32},
			{ShardID: "s-3", SizeBytes: 260, TombstoneRatio: 0.10},
		})

		require.Len(t, got, 2)
		require.Equal(t, []string{"s-1", "s-2"}, []string{got[0].ShardID, got[1].ShardID})
	})

	t.Run("should compact on tombstone pressure", func(t *testing.T) {
		policy := sharding.Policy{CompactionEnabled: true, CompactionMinShardCount: 8, CompactionTombstoneRatio: 0.20}

		require.False(t, ShouldCompact(policy, []Shard{{ShardID: "s1"}}))
		require.True(t, ShouldCompact(policy, []Shard{{ShardID: "s1", TombstoneRatio: 0.25}, {ShardID: "s2"}}))
	})
}
