package kb

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// newTestMongoCollection returns a scratch Mongo collection scoped to the test.
// Skips the test when MINNOW_TEST_MONGO_URI is unset. The collection is dropped
// before use and on cleanup, and the client is disconnected on cleanup.
// The returned client is useful for tests that need to Ping or start sessions.
func newTestMongoCollection(t *testing.T, prefix string) (*mongo.Collection, *mongo.Client) {
	t.Helper()
	uri := os.Getenv("MINNOW_TEST_MONGO_URI")
	if uri == "" {
		t.Skip("MINNOW_TEST_MONGO_URI not set; skipping Mongo integration test")
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	require.NoError(t, err)
	require.NoError(t, client.Ping(context.Background(), nil))
	coll := client.Database("minnow_test").Collection(prefix + "_" + t.Name())
	_ = coll.Drop(context.Background())
	t.Cleanup(func() {
		_ = coll.Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	return coll, client
}
