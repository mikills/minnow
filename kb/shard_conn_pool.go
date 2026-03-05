package kb

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"
)

// shardConn is a pooled DuckDB connection for a single shard file.
// Callers must hold mu while using db to serialize access.
type shardConn struct {
	db      *sql.DB
	mu      sync.Mutex
	lastUse time.Time
}

// shardConnPool keeps warm DuckDB connections keyed by local file path.
type shardConnPool struct {
	mu      sync.Mutex
	entries map[string]*shardConn
}

// GetOrOpen returns a pooled connection for localPath, creating one via openFn
// if not already cached. The caller MUST lock the returned shardConn.mu before
// using shardConn.db and unlock it after.
func (p *shardConnPool) GetOrOpen(ctx context.Context, localPath string,
	openFn func(ctx context.Context, path string) (*sql.DB, error)) (*shardConn, error) {

	p.mu.Lock()
	if p.entries == nil {
		p.entries = make(map[string]*shardConn)
	}
	if sc, ok := p.entries[localPath]; ok {
		p.mu.Unlock()
		sc.mu.Lock()
		sc.lastUse = time.Now()
		sc.mu.Unlock()
		return sc, nil
	}
	p.mu.Unlock()

	db, err := openFn(ctx, localPath)
	if err != nil {
		return nil, err
	}

	sc := &shardConn{db: db, lastUse: time.Now()}

	p.mu.Lock()
	// Another goroutine may have raced and inserted first.
	if existing, ok := p.entries[localPath]; ok {
		p.mu.Unlock()
		_ = db.Close()
		existing.mu.Lock()
		existing.lastUse = time.Now()
		existing.mu.Unlock()
		return existing, nil
	}
	p.entries[localPath] = sc
	p.mu.Unlock()

	return sc, nil
}

// CloseByPrefix closes and removes all connections whose key starts with prefix.
// Called by cache eviction before deleting shard files from disk.
func (p *shardConnPool) CloseByPrefix(prefix string) {
	p.mu.Lock()
	var toClose []*shardConn
	for key, sc := range p.entries {
		if strings.HasPrefix(key, prefix) {
			toClose = append(toClose, sc)
			delete(p.entries, key)
		}
	}
	p.mu.Unlock()

	for _, sc := range toClose {
		sc.mu.Lock()
		_ = sc.db.Close()
		sc.mu.Unlock()
	}
}

// CloseAll closes every pooled connection. Called on shutdown.
func (p *shardConnPool) CloseAll() {
	p.mu.Lock()
	entries := p.entries
	p.entries = nil
	p.mu.Unlock()

	for _, sc := range entries {
		sc.mu.Lock()
		_ = sc.db.Close()
		sc.mu.Unlock()
	}
}
