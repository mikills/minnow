package vectorplan

import (
	"fmt"
	"testing"

	kb "github.com/mikills/minnow/kb"
	"github.com/stretchr/testify/require"
)

func BenchmarkSelectTopShardsClusteredRecall(b *testing.B) {
	for _, fanout := range []int{1, 3, 6} {
		b.Run(fmt.Sprintf("fanout=%d", fanout), func(b *testing.B) {
			shards, queries := clusteredShardRecallFixture(128, 32)
			b.ReportAllocs()
			hits := 0
			for i := 0; b.Loop(); i++ {
				query := queries[i%len(queries)]
				selected := SelectTopShards(shards, query.vector, fanout)
				require.NotEmpty(b, selected)
				if selectedContainsShard(selected, query.wantShardID) {
					hits++
				}
			}
			b.ReportMetric(float64(hits)/float64(max(b.N, 1)), "recall@fanout")
		})
	}
}

type centroidQuery struct {
	vector      []float32
	wantShardID string
}

func clusteredShardRecallFixture(shardCount int, dim int) ([]kb.SnapshotShardMetadata, []centroidQuery) {
	shards := make([]kb.SnapshotShardMetadata, 0, shardCount)
	queries := make([]centroidQuery, 0, shardCount)
	for i := range shardCount {
		centroid := make([]float32, dim)
		centroid[i%dim] = 1
		centroid[(i*7+3)%dim] = 0.25
		shardID := fmt.Sprintf("shard-%03d", i)
		shards = append(
			shards,
			kb.SnapshotShardMetadata{ShardID: shardID, Key: shardID + ".duckdb", VectorRows: 100, Centroid: centroid},
		)
		query := append([]float32(nil), centroid...)
		queries = append(queries, centroidQuery{vector: query, wantShardID: shardID})
	}
	return shards, queries
}

func selectedContainsShard(selected []kb.SnapshotShardMetadata, shardID string) bool {
	for _, shard := range selected {
		if shard.ShardID == shardID {
			return true
		}
	}
	return false
}
