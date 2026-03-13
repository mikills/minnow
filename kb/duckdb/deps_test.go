package duckdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kb "github.com/mikills/kbcore/kb"
)

func TestNewDepsFromKB(t *testing.T) {
	t.Run("safe_defaults", func(t *testing.T) {
		h := kb.NewTestHarness(t, "kb-deps-defaults").Setup()
		t.Cleanup(h.Cleanup)

		deps := NewDepsFromKB(h.KB())

		assert.True(t, deps.OfflineExt, "OfflineExt should default to true")
		// ExtensionDir should be set to whatever ResolveExtensionDir returns.
		// In this repo it finds .duckdb/extensions; in CI it may be empty.
		// The key invariant is that it equals the resolver output, not that it's non-empty.
		assert.Equal(t, ResolveExtensionDir(), deps.ExtensionDir, "ExtensionDir should default to ResolveExtensionDir()")
	})

	t.Run("options_override_defaults", func(t *testing.T) {
		h := kb.NewTestHarness(t, "kb-deps-override").Setup()
		t.Cleanup(h.Cleanup)

		customDir := t.TempDir()
		deps := NewDepsFromKB(h.KB(),
			WithExtensionDir(customDir),
			WithOfflineExt(false),
			WithMemoryLimit("256MB"),
		)

		assert.Equal(t, customDir, deps.ExtensionDir, "WithExtensionDir should override default")
		assert.False(t, deps.OfflineExt, "WithOfflineExt(false) should override default")
		assert.Equal(t, "256MB", deps.MemoryLimit, "WithMemoryLimit should set memory limit")
	})

	t.Run("acquire_write_lease_wired", func(t *testing.T) {
		h := kb.NewTestHarness(t, "kb-deps-lease").Setup()
		t.Cleanup(h.Cleanup)

		deps := NewDepsFromKB(h.KB())
		require.NotNil(t, deps.AcquireWriteLease, "AcquireWriteLease should be wired from KB")
	})
}
