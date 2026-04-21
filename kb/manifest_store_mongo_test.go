package kb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoManifestStore(t *testing.T) {
	t.Run("contract", func(t *testing.T) {
		runManifestStoreTests(t, func(t *testing.T) ManifestStore {
			return newTestMongoManifestStore(t)
		})
	})

	t.Run("nil collection returns ErrInvalidManifestStore", func(t *testing.T) {
	store := NewMongoManifestStore(nil)
	ctx := context.Background()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Get", func() error { _, err := store.Get(ctx, "kb"); return err }},
		{"HeadVersion", func() error { _, err := store.HeadVersion(ctx, "kb"); return err }},
		{"UpsertIfMatch", func() error { _, err := store.UpsertIfMatch(ctx, "kb", SnapshotShardManifest{}, ""); return err }},
		{"Delete", func() error { return store.Delete(ctx, "kb") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.fn(), ErrInvalidManifestStore)
		})
	}
	})
}

func newTestMongoManifestStore(t *testing.T) *MongoManifestStore {
	t.Helper()

	uri := os.Getenv("MINNOW_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("MINNOW_TEST_MONGO_URI not set; skipping Mongo integration test")
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err, "mongo connect")

	ctx := context.Background()
	require.NoError(t, client.Ping(ctx, nil), "mongo ping")

	dbName := "minnow_test"
	collName := "manifests_" + t.Name()
	coll := client.Database(dbName).Collection(collName)

	// Clean up collection before and after test.
	_ = coll.Drop(ctx)
	t.Cleanup(func() {
		_ = coll.Drop(ctx)
		_ = client.Disconnect(ctx)
	})

	return NewMongoManifestStore(coll)
}
