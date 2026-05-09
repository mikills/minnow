package duckdb

import (
	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/kb/duckdb/internal/compact"
)

func buildCompactedManifest(
	kbID string,
	current *kb.SnapshotShardManifest,
	replaced []kb.SnapshotShardMetadata,
	replacement kb.SnapshotShardMetadata,
) kb.SnapshotShardManifest {
	return compact.BuildManifest(kbID, current, replaced, replacement)
}

func selectCompactionCandidatesWithReason(
	policy kb.ShardingPolicy,
	manifest *kb.SnapshotShardManifest,
) ([]kb.SnapshotShardMetadata, string) {
	return compact.SelectCandidatesWithReason(policy, manifest)
}
