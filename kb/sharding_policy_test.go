package kb

import "testing"

func TestNormalizeShardingPolicy(t *testing.T) {
	t.Run("defaults_to_enabled", func(t *testing.T) {
		policy := NormalizeShardingPolicy(ShardingPolicy{})
		if !policy.CompactionEnabled {
			t.Fatal("expected default compaction to be enabled")
		}
	})

	t.Run("honors_explicit_disable", func(t *testing.T) {
		policy := NormalizeShardingPolicy(ShardingPolicy{CompactionEnabled: false, CompactionEnabledSet: true})
		if policy.CompactionEnabled {
			t.Fatal("expected compaction to be disabled")
		}
	})

	t.Run("with_compaction_enabled_option_disables", func(t *testing.T) {
		kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(), WithCompactionEnabled(false))
		resolved := NormalizeShardingPolicy(kb.ShardingPolicy)
		if resolved.CompactionEnabled {
			t.Fatal("expected normalized policy to preserve explicit disable")
		}
	})
}
