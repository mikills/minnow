package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// Callers must hold mu while using db.
type shardConn struct {
	db               *sql.DB
	mu               sync.Mutex
	lastUse          time.Time
	metadataExpr     string
	vectorStatements []shardVectorStatement
}

var errShardConnPoolEvicting = errors.New("DuckDB shard cache path is being evicted")

// shardConnPool keeps warm DuckDB connections keyed by local file path.
type shardConnPool struct {
	mu          sync.Mutex
	entries     map[string]*shardConn
	evicting    map[string]*shardEviction
	generations map[string]uint64
	inFlight    map[string]*shardOpen
	openChanged *sync.Cond
}

type shardEviction struct {
	count int
	done  chan struct{}
}

type shardOpen struct {
	done chan struct{}
	ctx  context.Context
	conn *shardConn
	err  error
}

// GetOrOpen returns a locked connection for localPath. The caller must unlock
// shardConn.mu when done with shardConn.db.
func (p *shardConnPool) GetOrOpen(ctx context.Context, localPath string, maxEntries int,
	openFn func(ctx context.Context, path string) (*sql.DB, error)) (*shardConn, error) {

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p.mu.Lock()
		generation := p.generationLocked(localPath)
		if p.pathEvictingLocked(localPath) {
			p.mu.Unlock()
			return nil, errShardConnPoolEvicting
		}
		if p.entries == nil {
			p.entries = make(map[string]*shardConn)
		}
		if sc, ok := p.entries[localPath]; ok {
			sc.mu.Lock()
			sc.lastUse = time.Now()
			p.mu.Unlock()
			return sc, nil
		}
		if pending := p.inFlight[localPath]; pending != nil {
			p.mu.Unlock()
			conn, err := p.waitForOpen(ctx, localPath, pending)
			if ctx.Err() == nil && pending.ctx.Err() != nil &&
				(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				// The opener belongs to another request. Its cancellation must
				// not cancel a still-live follower; retry with this request's context.
				continue
			}
			return conn, err
		}
		if p.inFlight == nil {
			p.inFlight = make(map[string]*shardOpen)
		}
		pending := &shardOpen{done: make(chan struct{}), ctx: ctx}
		p.inFlight[localPath] = pending
		p.mu.Unlock()

		db, err := openFn(ctx, localPath)
		if err != nil {
			// Native preparation may report only an interrupt, not wrap the
			// request's cancellation. Snapshot that cause before publishing the
			// failure so live followers can retry without losing the native error.
			if ctxErr := ctx.Err(); ctxErr != nil {
				err = errors.Join(err, ctxErr)
			}
			p.mu.Lock()
			p.finishOpenLocked(localPath, err)
			p.mu.Unlock()
			return nil, err
		}

		sc := &shardConn{db: db, lastUse: time.Now()}

		p.mu.Lock()
		changed := p.pathEvictingLocked(localPath) || p.generationLocked(localPath) != generation
		if changed {
			p.mu.Unlock()
			if closeErr := db.Close(); closeErr != nil {
				slog.Default().Warn("close shard connection opened during eviction failed", logKeyError, closeErr)
			}
			p.mu.Lock()
			p.finishOpenLocked(localPath, errShardConnPoolEvicting)
			p.mu.Unlock()
			return nil, errShardConnPoolEvicting
		}
		sc.mu.Lock()
		p.entries[localPath] = sc
		pending.conn = sc
		p.finishOpenLocked(localPath, nil)
		toClose := p.takeOverflowLocked(localPath, maxEntries)
		p.mu.Unlock()

		for _, stale := range toClose {
			stale.mu.Lock()
			if err := stale.close(); err != nil {
				slog.Default().Warn("close LRU shard connection failed", logKeyError, err)
			}
			stale.mu.Unlock()
		}
		return sc, nil
	}
}

// Waiters do not open another native database or cancel the opener. Recheck
// identity under the pool lock: a completed open may already have been evicted,
// including by an eviction whose barrier has since been released.
func (p *shardConnPool) waitForOpen(ctx context.Context, path string, pending *shardOpen) (*shardConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pending.done:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pending.err != nil {
		return nil, pending.err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pathEvictingLocked(path) || p.entries[path] != pending.conn {
		return nil, errShardConnPoolEvicting
	}
	sc := pending.conn
	sc.mu.Lock()
	sc.lastUse = time.Now()
	return sc, nil
}

// maxEntries comes from the caller's memory plan.
func (p *shardConnPool) takeOverflowLocked(protectedPath string, maxEntries int) []*shardConn {
	var toClose []*shardConn
	for len(p.entries) > maxEntries {
		var oldestPath string
		var oldest *shardConn
		for path, candidate := range p.entries {
			if path == protectedPath {
				continue
			}
			if oldest == nil || candidate.lastUse.Before(oldest.lastUse) {
				oldestPath = path
				oldest = candidate
			}
		}
		if oldest == nil {
			break
		}
		delete(p.entries, oldestPath)
		toClose = append(toClose, oldest)
	}
	return toClose
}

// BeginEviction blocks new opens beneath prefix, closes existing handles, and
// returns a release function. The caller must keep the barrier until files are
// removed from disk.
func (p *shardConnPool) BeginEviction(prefix string) func() {
	prefix = strings.TrimSuffix(prefix, string(os.PathSeparator))
	p.mu.Lock()
	if p.openChanged == nil {
		p.openChanged = sync.NewCond(&p.mu)
	}
	if p.evicting == nil {
		p.evicting = make(map[string]*shardEviction)
	}
	state := p.evicting[prefix]
	if state == nil {
		state = &shardEviction{done: make(chan struct{})}
		p.evicting[prefix] = state
	}
	state.count++
	if p.generations == nil {
		p.generations = make(map[string]uint64)
	}
	p.generations[prefix]++
	var toClose []*shardConn
	for key, sc := range p.entries {
		if pathWithinPrefix(key, prefix) {
			toClose = append(toClose, sc)
			delete(p.entries, key)
		}
	}
	p.mu.Unlock()

	for _, sc := range toClose {
		sc.mu.Lock()
		if err := sc.close(); err != nil {
			slog.Default().Warn("close shard connection failed", logKeyError, err)
		}
		sc.mu.Unlock()
	}
	p.mu.Lock()
	for p.hasOpenWithinPrefixLocked(prefix) {
		p.openChanged.Wait()
	}
	p.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			p.mu.Lock()
			state := p.evicting[prefix]
			if state != nil {
				state.count--
				if state.count == 0 {
					delete(p.evicting, prefix)
					close(state.done)
					if !p.hasEntriesWithinPrefixLocked(prefix) && !p.hasOpenWithinPrefixLocked(prefix) {
						delete(p.generations, prefix)
					}
				}
			}
			p.mu.Unlock()
		})
	}
}

// CloseByPrefix closes current handles without retaining an eviction barrier.
func (p *shardConnPool) CloseByPrefix(prefix string) {
	p.BeginEviction(prefix)()
}

func (p *shardConnPool) waitForEviction(ctx context.Context, path string) error {
	for {
		p.mu.Lock()
		var done <-chan struct{}
		for prefix, state := range p.evicting {
			if pathWithinPrefix(path, prefix) {
				done = state.done
				break
			}
		}
		p.mu.Unlock()
		if done == nil {
			return nil
		}
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *shardConnPool) finishOpenLocked(path string, err error) {
	pending := p.inFlight[path]
	delete(p.inFlight, path)
	pending.err = err
	close(pending.done)
	if p.openChanged != nil {
		p.openChanged.Broadcast()
	}
	for prefix := range p.generations {
		if pathWithinPrefix(path, prefix) && p.evicting[prefix] == nil &&
			!p.hasEntriesWithinPrefixLocked(prefix) && !p.hasOpenWithinPrefixLocked(prefix) {
			delete(p.generations, prefix)
		}
	}
}

func (p *shardConnPool) hasOpenWithinPrefixLocked(prefix string) bool {
	for path := range p.inFlight {
		if pathWithinPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (p *shardConnPool) hasEntriesWithinPrefixLocked(prefix string) bool {
	for path := range p.entries {
		if pathWithinPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func (p *shardConnPool) generationLocked(path string) uint64 {
	var generation uint64
	for prefix, value := range p.generations {
		if pathWithinPrefix(path, prefix) {
			generation += value
		}
	}
	return generation
}

func (p *shardConnPool) pathEvictingLocked(path string) bool {
	for prefix := range p.evicting {
		if pathWithinPrefix(path, prefix) {
			return true
		}
	}
	return false
}

func pathWithinPrefix(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+string(os.PathSeparator))
}

// CloseAll closes every pooled connection. Called on shutdown.
func (p *shardConnPool) CloseAll() {
	p.mu.Lock()
	entries := p.entries
	p.entries = nil
	p.mu.Unlock()

	for _, sc := range entries {
		sc.mu.Lock()
		if err := sc.close(); err != nil {
			slog.Default().Warn("close shard connection failed", logKeyError, err)
		}
		sc.mu.Unlock()
	}
}
