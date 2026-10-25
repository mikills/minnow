package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Pool control tests use no native database or timed performance workload.
type poolTestConnector struct{ closed atomic.Int64 }

func (*poolTestConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("pool control test must not execute SQL")
}
func (*poolTestConnector) Driver() driver.Driver { return poolTestDriver{} }
func (c *poolTestConnector) Close() error        { c.closed.Add(1); return nil }

type poolTestDriver struct{}

func (poolTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("pool control test must not execute SQL")
}

type poolWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *poolWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type poolOpenResult struct {
	conn *shardConn
	err  error
}

func startPoolOpen(pool *shardConnPool, ctx context.Context, path string, open func(context.Context, string) (*sql.DB, error)) <-chan poolOpenResult {
	result := make(chan poolOpenResult, 1)
	go func() {
		conn, err := pool.GetOrOpen(ctx, path, 4, open)
		// Return unlocked so several waiters can all finish.
		if conn != nil {
			conn.mu.Unlock()
		}
		result <- poolOpenResult{conn, err}
	}()
	return result
}

func TestShardOpen(t *testing.T) {
	t.Run("coalesces_same_path", func(t *testing.T) {
		var pool shardConnPool
		t.Cleanup(pool.CloseAll)
		started, resume := make(chan struct{}), make(chan struct{})
		connector := &poolTestConnector{}
		var opens atomic.Int64
		open := func(context.Context, string) (*sql.DB, error) {
			opens.Add(1)
			close(started)
			<-resume
			return sql.OpenDB(connector), nil
		}
		leader := startPoolOpen(&pool, t.Context(), "same", open)
		<-started
		var waiters []<-chan poolOpenResult
		for range 8 {
			ctx := &poolWaitContext{Context: t.Context(), waiting: make(chan struct{})}
			waiters = append(waiters, startPoolOpen(&pool, ctx, "same", open))
			<-ctx.waiting
		}
		// A different path is not serialized behind the slow initialization.
		other, err := pool.GetOrOpen(t.Context(), "other", 4, func(context.Context, string) (*sql.DB, error) {
			return sql.OpenDB(&poolTestConnector{}), nil
		})
		require.NoError(t, err)
		other.mu.Unlock()
		close(resume)
		first := <-leader
		require.NoError(t, first.err)
		for _, waiter := range waiters {
			got := <-waiter
			require.NoError(t, got.err)
			require.Same(t, first.conn, got.conn)
		}
		require.EqualValues(t, 1, opens.Load())
		pool.CloseAll()
		require.EqualValues(t, 1, connector.closed.Load())
	})

	t.Run("waiter_cancellation", func(t *testing.T) {
		var pool shardConnPool
		t.Cleanup(pool.CloseAll)
		started, resume := make(chan struct{}), make(chan struct{})
		open := func(context.Context, string) (*sql.DB, error) {
			close(started)
			<-resume
			return sql.OpenDB(&poolTestConnector{}), nil
		}
		leader := startPoolOpen(&pool, t.Context(), "same", open)
		<-started
		canceled, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctx := &poolWaitContext{Context: canceled, waiting: make(chan struct{})}
		waiter := startPoolOpen(&pool, ctx, "same", open)
		<-ctx.waiting
		cancel()
		require.ErrorIs(t, (<-waiter).err, context.Canceled)
		close(resume)
		require.NoError(t, (<-leader).err)
	})

	t.Run("failure_and_retry", func(t *testing.T) {
		var pool shardConnPool
		t.Cleanup(pool.CloseAll)
		started, resume := make(chan struct{}), make(chan struct{})
		failure := errors.New("load failed")
		var opens atomic.Int64
		open := func(context.Context, string) (*sql.DB, error) {
			if opens.Add(1) == 1 {
				close(started)
				<-resume
				return nil, failure
			}
			return sql.OpenDB(&poolTestConnector{}), nil
		}
		leader := startPoolOpen(&pool, t.Context(), "same", open)
		<-started
		ctx := &poolWaitContext{Context: t.Context(), waiting: make(chan struct{})}
		waiter := startPoolOpen(&pool, ctx, "same", open)
		<-ctx.waiting
		close(resume)
		require.ErrorIs(t, (<-leader).err, failure)
		require.ErrorIs(t, (<-waiter).err, failure)
		require.NoError(t, (<-startPoolOpen(&pool, t.Context(), "same", open)).err)
		require.EqualValues(t, 2, opens.Load())
	})

	t.Run("opener_cancellation_does_not_cancel_waiter", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			err  error
		}{
			{name: "context_error", err: context.Canceled},
			{name: "native_interrupt", err: errors.New("INTERRUPT Error: Interrupted!")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var pool shardConnPool
				t.Cleanup(pool.CloseAll)
				started := make(chan struct{})
				var opens atomic.Int64
				open := func(ctx context.Context, _ string) (*sql.DB, error) {
					if opens.Add(1) == 1 {
						close(started)
						<-ctx.Done()
						// Native preparation can report an interrupt without wrapping
						// the canceled request's context error.
						return nil, tc.err
					}
					return sql.OpenDB(&poolTestConnector{}), nil
				}
				canceled, cancel := context.WithCancel(t.Context())
				defer cancel()
				leader := startPoolOpen(&pool, canceled, "same", open)
				<-started
				ctx := &poolWaitContext{Context: t.Context(), waiting: make(chan struct{})}
				waiter := startPoolOpen(&pool, ctx, "same", open)
				<-ctx.waiting
				cancel()
				leaderResult := <-leader
				require.ErrorIs(t, leaderResult.err, context.Canceled)
				require.ErrorIs(t, leaderResult.err, tc.err, "preserve the original initialization error")
				require.NoError(t, (<-waiter).err)
				require.EqualValues(t, 2, opens.Load())
			})
		}
	})

	t.Run("eviction_drains_shared_open", func(t *testing.T) {
		var pool shardConnPool
		dir := t.TempDir()
		path := filepath.Join(dir, "shard.duckdb")
		started, resume := make(chan struct{}), make(chan struct{})
		connector := &poolTestConnector{}
		open := func(context.Context, string) (*sql.DB, error) {
			close(started)
			<-resume
			return sql.OpenDB(connector), nil
		}
		leader := startPoolOpen(&pool, t.Context(), path, open)
		<-started
		ctx := &poolWaitContext{Context: t.Context(), waiting: make(chan struct{})}
		waiter := startPoolOpen(&pool, ctx, path, open)
		<-ctx.waiting
		releaseReady := make(chan func(), 1)
		go func() { releaseReady <- pool.BeginEviction(dir) }()
		require.Eventually(t, func() bool {
			pool.mu.Lock()
			defer pool.mu.Unlock()
			return pool.pathEvictingLocked(path)
		}, 2*time.Second, time.Millisecond)
		close(resume)
		release := <-releaseReady
		defer release()
		require.ErrorIs(t, (<-leader).err, errShardConnPoolEvicting)
		require.ErrorIs(t, (<-waiter).err, errShardConnPoolEvicting)
		require.EqualValues(t, 1, connector.closed.Load(), "eviction returned before the native handle closed")
		require.Empty(t, pool.entries)
		require.Empty(t, pool.inFlight)
	})

	t.Run("completed_eviction_rejects_late_waiter", func(t *testing.T) {
		var pool shardConnPool
		t.Cleanup(pool.CloseAll)
		path := filepath.Join(t.TempDir(), "shard.duckdb")
		conn, err := pool.GetOrOpen(t.Context(), path, 4, func(context.Context, string) (*sql.DB, error) {
			return sql.OpenDB(&poolTestConnector{}), nil
		})
		require.NoError(t, err)
		conn.mu.Unlock()
		pending := &shardOpen{done: make(chan struct{}), conn: conn}
		close(pending.done)
		pool.BeginEviction(filepath.Dir(path))()
		got, err := pool.waitForOpen(t.Context(), path, pending)
		require.Nil(t, got)
		require.ErrorIs(t, err, errShardConnPoolEvicting)
	})
}
