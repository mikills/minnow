package kb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// DeleteKnowledgeBase removes the manifest and known shard/cache/media state for
// a KB. Event history is intentionally retained for auditability.
//
// Ordering: the manifest is deleted FIRST so callers see a consistent "gone"
// state even if downstream blob/cache/media cleanup partially fails. After the
// manifest is deleted, shard, cache, and media cleanup are best-effort: every
// step runs to completion and any failures are joined into the returned error
// so operators see the full picture instead of just the first failure. Orphan
// blobs left behind by a partial failure can be reclaimed by the GC sweep.
func (l *KB) DeleteKnowledgeBase(ctx context.Context, kbID string) error {
	if kbID == "" {
		return fmt.Errorf("kb_id required")
	}
	if l.ManifestStore == nil {
		return fmt.Errorf("manifest store is not configured")
	}

	manifest, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil && !errors.Is(err, ErrManifestNotFound) {
		return fmt.Errorf("read manifest %q: %w", kbID, err)
	}

	if err := l.ManifestStore.Delete(ctx, kbID); err != nil {
		return fmt.Errorf("delete manifest %q: %w", kbID, err)
	}

	var cleanupErrs []error
	if manifest != nil {
		for _, shard := range manifest.Manifest.Shards {
			if shard.Key == "" {
				continue
			}
			if err := l.BlobStore.Delete(ctx, shard.Key); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("delete shard %s: %w", shard.Key, err))
			}
		}
	}

	if l.CacheDir != "" {
		if err := os.RemoveAll(filepath.Join(l.CacheDir, kbID)); err != nil && !os.IsNotExist(err) {
			cleanupErrs = append(cleanupErrs, fmt.Errorf("remove cache dir for %q: %w", kbID, err))
		}
		// Recompute current cache bytes so the metric does not drift after
		// removing this KB's cache subtree. We cannot just zero it (other KBs
		// may still occupy space).
		_, total := l.collectCacheEntries()
		l.recordCacheBytesCurrent(total)
	}

	if l.MediaStore != nil {
		for after := ""; ; {
			page, listErr := l.MediaStore.List(ctx, kbID, "", after, 500)
			if listErr != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("list media for kb delete: %w", listErr))
				break
			}
			for _, item := range page.Items {
				if err := l.MediaStore.Delete(ctx, item.ID); err != nil {
					cleanupErrs = append(cleanupErrs, fmt.Errorf("delete media %s: %w", item.ID, err))
				}
			}
			if page.NextToken == "" {
				break
			}
			after = page.NextToken
		}
	}

	if len(cleanupErrs) > 0 {
		return fmt.Errorf("knowledge base %q deleted with cleanup errors: %w", kbID, errors.Join(cleanupErrs...))
	}
	return nil
}

// TombstoneMedia marks a media object as deleted without removing its metadata.
func (l *KB) TombstoneMedia(ctx context.Context, mediaID string) error {
	if mediaID == "" {
		return fmt.Errorf("media_id required")
	}
	if l.MediaStore == nil {
		return fmt.Errorf("media subsystem not configured")
	}
	return l.MediaStore.UpdateState(ctx, mediaID, MediaStateTombstoned, l.Clock.Now().UnixMilli())
}

// ClearCache removes all local cache entries regardless of TTL or size policy.
func (l *KB) ClearCache() error {
	if l.CacheDir == "" {
		return nil
	}
	entries, err := os.ReadDir(l.CacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(l.CacheDir, entry.Name())); err != nil {
			return err
		}
	}
	l.recordCacheBytesCurrent(0)
	return nil
}
