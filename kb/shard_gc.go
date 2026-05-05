package kb

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// DefaultShardGCGraceWindow is the minimum time to wait after a shard is
// replaced before it can be deleted. This allows in-flight readers to finish.
const DefaultShardGCGraceWindow = 2 * time.Minute

// DefaultShardGCRetryDelay is the delay before retrying a failed GC operation
// (manifest download failure, delete failure, or shard still referenced).
const DefaultShardGCRetryDelay = 10 * time.Second

// delayedShardGCEntry represents a shard queued for delayed garbage collection.
type delayedShardGCEntry struct {
	KBID      string                // knowledge base the shard belongs to
	Shard     SnapshotShardMetadata // metadata of the shard to delete
	NotBefore time.Time             // earliest time the shard can be deleted
}

// ShardGCSweepResult summarizes the outcome of one delayed GC sweep.
//
// Deleted is the count of shards successfully removed. Retried is the count of
// shards that failed and were re-queued. Pending is the total count of entries
// remaining in the queue after the sweep.
type ShardGCSweepResult struct {
	Deleted int
	Retried int
	Pending int
}

// enqueueReplacedShardsForGC adds replaced shards to the GC queue with a grace
// window before they can be deleted.
//
// If a shard is already in the queue, its NotBefore time is extended rather
// than creating a duplicate entry. This handles cases where the same shard is
// replaced multiple times before GC runs.
//
// Called by compaction after successfully publishing a new manifest.
func (l *KB) enqueueReplacedShardsForGC(kbID string, shards []SnapshotShardMetadata, now time.Time) {
	if kbID == "" || len(shards) == 0 {
		return
	}

	if now.IsZero() {
		now = l.Clock.Now()
	}

	notBefore := now.Add(DefaultShardGCGraceWindow)

	l.mu.Lock()
	defer l.mu.Unlock()

	for _, shard := range shards {
		l.enqueueShardGCLocked(kbID, shard, notBefore)
	}
}

func (l *KB) enqueueShardGCLocked(kbID string, shard SnapshotShardMetadata, notBefore time.Time) {
	if shard.Key == "" {
		return
	}
	if l.extendShardGCLocked(kbID, shard.Key, notBefore) {
		return
	}
	l.shardGC = append(l.shardGC, delayedShardGCEntry{KBID: kbID, Shard: shard, NotBefore: notBefore})
}

func (l *KB) extendShardGCLocked(kbID string, shardKey string, notBefore time.Time) bool {
	for i := range l.shardGC {
		entry := &l.shardGC[i]
		if entry.KBID == kbID && entry.Shard.Key == shardKey {
			if notBefore.After(entry.NotBefore) {
				entry.NotBefore = notBefore
			}
			return true
		}
	}
	return false
}

// EnqueueReplacedShardsForGC exposes delayed shard GC queueing for backend-owned
// compaction implementations.
func (l *KB) EnqueueReplacedShardsForGC(kbID string, shards []SnapshotShardMetadata, now time.Time) {
	l.enqueueReplacedShardsForGC(kbID, shards, now)
}

// deleteShardObject removes a shard file from blob storage.
//
// Returns nil if the file is already deleted (idempotent).
func (l *KB) deleteShardObject(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}

	err := l.BlobStore.Delete(ctx, key)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if errors.Is(err, ErrBlobNotFound) {
		return nil
	}

	return err
}

// SweepDelayedShardGC processes the GC queue and deletes replaced shard files
// that have passed their grace window.
//
// For each queued entry:
//  1. Skip if still within grace window (NotBefore > now).
//  2. Download the current manifest to verify shard is no longer referenced.
//  3. If shard is still in manifest, re-queue with retry delay (may have been
//     re-added by concurrent operation).
//  4. Delete the shard file from blob storage.
//  5. On delete failure, re-queue with retry delay.
//
// The sweep is atomic with respect to the queue: it takes a snapshot at the
// start and replaces the queue with remaining entries at the end.
//
// Returns the first error encountered (but continues processing all entries).
func (l *KB) SweepDelayedShardGC(ctx context.Context, now time.Time) (ShardGCSweepResult, error) {
	if now.IsZero() {
		now = l.Clock.Now()
	}

	l.mu.Lock()
	queue := append([]delayedShardGCEntry(nil), l.shardGC...)
	l.mu.Unlock()

	if len(queue) == 0 {
		return ShardGCSweepResult{}, nil
	}

	state := shardGCSweepState{activeKeysByKB: make(map[string]map[string]struct{}), next: make([]delayedShardGCEntry, 0, len(queue))}
	for _, entry := range queue {
		if err := ctx.Err(); err != nil {
			return state.result, err
		}
		l.sweepShardGCEntry(ctx, now, entry, &state)
	}

	next := state.next
	result := state.result
	firstErr := state.firstErr

	result.Pending = len(next)
	l.mu.Lock()
	l.shardGC = next
	l.mu.Unlock()

	if result.Deleted > 0 || result.Retried > 0 || result.Pending > 0 {
		slog.Default().InfoContext(ctx, "completed deferred shard GC sweep", "reason", "gc_sweep", "deleted", result.Deleted, "retried", result.Retried, "pending", result.Pending)
	}

	return result, firstErr
}

type shardGCSweepState struct {
	activeKeysByKB map[string]map[string]struct{}
	next           []delayedShardGCEntry
	result         ShardGCSweepResult
	firstErr       error
}

func (l *KB) sweepShardGCEntry(ctx context.Context, now time.Time, entry delayedShardGCEntry, state *shardGCSweepState) {
	if now.Before(entry.NotBefore) {
		slog.Default().InfoContext(ctx, "deferred shard GC pending grace window", "kb_id", entry.KBID, "reason", "grace_window", "shard_key", entry.Shard.Key, "not_before", entry.NotBefore)
		state.next = append(state.next, entry)
		return
	}
	activeKeys, err := l.activeShardKeysForGC(ctx, entry, state.activeKeysByKB)
	if err != nil {
		state.retry(entry, now, err)
		return
	}
	if _, stillReferenced := activeKeys[entry.Shard.Key]; stillReferenced {
		slog.Default().InfoContext(ctx, "deferred shard GC skipped referenced shard", "kb_id", entry.KBID, "reason", "still_referenced", "shard_key", entry.Shard.Key)
		state.retry(entry, now, nil)
		return
	}
	if err := l.deleteShardObject(ctx, entry.Shard.Key); err != nil {
		slog.Default().WarnContext(ctx, "deferred shard GC delete failed", "kb_id", entry.KBID, "reason", "delete_failed", "shard_key", entry.Shard.Key, "error", err)
		state.retry(entry, now, fmt.Errorf("delete replaced shard %s: %w", entry.Shard.Key, err))
		return
	}
	state.result.Deleted++
	slog.Default().InfoContext(ctx, "deferred shard GC deleted shard", "kb_id", entry.KBID, "reason", "deleted", "shard_key", entry.Shard.Key)
}

func (s *shardGCSweepState) retry(entry delayedShardGCEntry, now time.Time, err error) {
	if err != nil && s.firstErr == nil {
		s.firstErr = err
	}
	entry.NotBefore = now.Add(DefaultShardGCRetryDelay)
	s.next = append(s.next, entry)
	s.result.Retried++
}

func (l *KB) activeShardKeysForGC(ctx context.Context, entry delayedShardGCEntry, cache map[string]map[string]struct{}) (map[string]struct{}, error) {
	if activeKeys, ok := cache[entry.KBID]; ok {
		return activeKeys, nil
	}
	doc, err := l.ManifestStore.Get(ctx, entry.KBID)
	if errors.Is(err, ErrManifestNotFound) {
		cache[entry.KBID] = map[string]struct{}{}
		return cache[entry.KBID], nil
	}
	if err != nil {
		slog.Default().WarnContext(ctx, "deferred shard GC manifest download failed", "kb_id", entry.KBID, "reason", "manifest_download_failed", "shard_key", entry.Shard.Key, "error", err)
		return nil, fmt.Errorf("download manifest for shard gc: %w", err)
	}
	activeKeys := activeShardKeys(doc.Manifest.Shards)
	cache[entry.KBID] = activeKeys
	return activeKeys, nil
}

func activeShardKeys(shards []SnapshotShardMetadata) map[string]struct{} {
	keys := make(map[string]struct{}, len(shards))
	for _, shard := range shards {
		if shard.Key != "" {
			keys[shard.Key] = struct{}{}
		}
	}
	return keys
}

// shardGCPendingCount returns the number of shards currently queued for GC.
// Used primarily for testing and metrics.
func (l *KB) shardGCPendingCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.shardGC)
}
