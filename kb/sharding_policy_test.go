package kb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeShardingPolicy(t *testing.T) {
	t.Run("defaults_to_enabled", func(t *testing.T) {
		policy := NormalizeShardingPolicy(ShardingPolicy{})
		require.True(t, policy.CompactionEnabled, "expected default compaction to be enabled")
	})

	t.Run("honors_explicit_disable", func(t *testing.T) {
		policy := NormalizeShardingPolicy(ShardingPolicy{CompactionEnabled: false, CompactionEnabledSet: true})
		require.False(t, policy.CompactionEnabled, "expected compaction to be disabled")
	})

	t.Run("with_compaction_enabled_option_disables", func(t *testing.T) {
		kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(), WithCompactionEnabled(false))
		resolved := NormalizeShardingPolicy(kb.ShardingPolicy)
		require.False(t, resolved.CompactionEnabled, "expected normalized policy to preserve explicit disable")
	})
}
