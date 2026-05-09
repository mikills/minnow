package shardcache

import (
	"testing"

	"github.com/stretchr/testify/require"

	kb "github.com/mikills/minnow/kb"
)

func TestFileName(t *testing.T) {
	got := FileName(kb.SnapshotShardMetadata{ShardID: "bad/id", Key: "k", Version: "v"})
	require.Contains(t, got, "bad_id")
	require.Contains(t, got, ".duckdb")
}
