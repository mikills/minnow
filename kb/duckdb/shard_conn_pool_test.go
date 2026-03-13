package duckdb

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

func TestShardConnPoolCloseByPrefix(t *testing.T) {
	var pool shardConnPool
	path := filepath.Join(t.TempDir(), "pool-test.duckdb")

	conn, err := pool.GetOrOpen(context.Background(), path, func(_ context.Context, p string) (*sql.DB, error) {
		return sql.Open("duckdb", p)
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		pool.CloseByPrefix(filepath.Dir(path))
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("CloseByPrefix returned before borrowed connection was released")
	case <-time.After(50 * time.Millisecond):
	}

	conn.mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("CloseByPrefix did not complete after borrowed connection release")
	}
}
