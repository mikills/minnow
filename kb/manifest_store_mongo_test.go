package kb_test

import (
	"context"
	"testing"

	. "github.com/mikills/minnow/kb"

	"github.com/stretchr/testify/require"
)

func TestMongoManifestStore(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		runManifestStoreTests(t, func(t *testing.T) ManifestStore {
			coll, _ := newTestMongoCollection(t, "manifests")
			return NewMongoManifestStore(coll)
		})
	})

	t.Run("nil collection is rejected", func(t *testing.T) {
		store := NewMongoManifestStore(nil)
		ctx := context.Background()
		tests := []struct {
			name string
			fn   func() error
		}{
			{"Get", func() error { _, err := store.Get(ctx, "kb"); return err }},
			{"HeadVersion", func() error { _, err := store.HeadVersion(ctx, "kb"); return err }},
			{
				"UpsertIfMatch",
				func() error { _, err := store.UpsertIfMatch(ctx, "kb", SnapshotShardManifest{}, ""); return err },
			},
			{"Delete", func() error { return store.Delete(ctx, "kb") }},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				require.ErrorIs(t, tt.fn(), ErrInvalidManifestStore)
			})
		}
	})
}
