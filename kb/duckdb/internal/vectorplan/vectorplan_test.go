package vectorplan

import (
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestMergeTopK(t *testing.T) {
	input := [][]kb.QueryResult{
		{{ID: "b", Content: "same", Distance: 0.5}},
		{{ID: "a", Content: "same", Distance: 0.5}, {ID: "c", Content: "alt", Distance: 0.5}},
	}
	got := MergeTopK(input, 2)
	require.Equal(t, []string{"a", "b"}, []string{got[0].ID, got[1].ID})
}

func TestPlanShardFanout(t *testing.T) {
	manifest := &kb.SnapshotShardManifest{Shards: []kb.SnapshotShardMetadata{
		{ShardID: "far", VectorRows: 1, Centroid: []float32{10}},
		{ShardID: "near", VectorRows: 1, Centroid: []float32{1}},
	}}
	plan := PlanShardFanout(
		kb.ShardingPolicy{QueryShardFanout: 1, QueryShardFanoutAdaptiveMax: 2, QueryShardParallelism: 3},
		manifest,
		[]float32{0},
	)
	require.Equal(t, 1, plan.Fanout)
	require.Equal(t, 1, plan.Parallelism)
	require.Equal(t, "near", plan.Shards[0].ShardID)
}
