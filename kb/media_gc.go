package kb

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"
)

// Tunables for media GC. Defaults mirror shard GC conventions and are
// overrideable via MediaGCConfig on the KB.
const (
	DefaultPendingMediaTTL     = 24 * time.Hour
	DefaultMediaTombstoneGrace = 1 * time.Hour
	DefaultUploadCompletionTTL = 1 * time.Hour
)

// MediaGCConfig tunes pending/tombstone timings.
type MediaGCConfig struct {
	PendingTTL       time.Duration
	TombstoneGrace   time.Duration
	UploadCompletion time.Duration
}

// MediaGCConfigOrDefault returns the configured values, falling back to the
// package defaults for any zero fields.
func (l *KB) MediaGCConfigOrDefault() MediaGCConfig {
	cfg := l.MediaGC
	if cfg.PendingTTL <= 0 {
		cfg.PendingTTL = DefaultPendingMediaTTL
	}
	if cfg.TombstoneGrace <= 0 {
		cfg.TombstoneGrace = DefaultMediaTombstoneGrace
	}
	if cfg.UploadCompletion <= 0 {
		cfg.UploadCompletion = DefaultUploadCompletionTTL
	}
	return cfg
}

// MediaGCResult summarises a single sweep.
type MediaGCResult struct {
	MarkedTombstone int
	Deleted         int
	Skipped         int
}

// iterMediaByState yields every media object of the given state by paging
// through the MediaStore so that a huge pending/tombstoned backlog does not
// materialise in a single slice.
func (l *KB) iterMediaByState(ctx context.Context, state MediaState, visit func(MediaObject) error) error {
	token := ""
	for {
		page, err := l.MediaStore.ListByState(ctx, "", state, token, MediaPageDefaultLimit)
		if err != nil {
			return err
		}
		for _, m := range page.Items {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(m); err != nil {
				return err
			}
		}
		if page.NextToken == "" {
			return nil
		}
		token = page.NextToken
	}
}

// SweepMediaGCMark transitions pending media past PendingMediaTTL to
// tombstoned when not referenced by any live manifest.
//
// "Live" means the current manifest only. Readers holding cached refs to
// older manifests may need to refetch if GC has reclaimed the media.
func (l *KB) SweepMediaGCMark(ctx context.Context, now time.Time) (MediaGCResult, error) {
	if l.MediaStore == nil {
		return MediaGCResult{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cfg := l.MediaGCConfigOrDefault()
	ttlCutoff := now.Add(-cfg.PendingTTL).UnixMilli()

	result := MediaGCResult{}
	activeByKB := make(map[string]map[string]struct{})

	err := l.iterMediaByState(ctx, MediaStatePending, func(m MediaObject) error {
		if m.CreatedAtUnixMs > ttlCutoff {
			result.Skipped++
			return nil
		}
		active, ok := activeByKB[m.KBID]
		if !ok {
			refs, err := l.liveMediaRefsForKB(ctx, m.KBID)
			if err != nil {
				slog.Default().Warn("media-gc-mark: manifest fetch failed", "kb_id", m.KBID, "error", err)
				result.Skipped++
				return nil
			}
			active = refs
			activeByKB[m.KBID] = active
		}
		if _, isReferenced := active[m.ID]; isReferenced {
			// Promote to active in-place; publish worker should have done
			// this, but mark phase is a backstop.
			_ = l.MediaStore.UpdateState(ctx, m.ID, MediaStateActive, 0)
			return nil
		}
		if err := l.MediaStore.UpdateState(ctx, m.ID, MediaStateTombstoned, now.UnixMilli()); err != nil {
			slog.Default().Warn("media-gc-mark: tombstone failed", "media_id", m.ID, "error", err)
			return nil
		}
		result.MarkedTombstone++
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

// SweepMediaGCDelete deletes blob + metadata for media tombstoned past
// the grace window.
func (l *KB) SweepMediaGCDelete(ctx context.Context, now time.Time) (MediaGCResult, error) {
	if l.MediaStore == nil || l.BlobStore == nil {
		return MediaGCResult{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cfg := l.MediaGCConfigOrDefault()
	cutoff := now.Add(-cfg.TombstoneGrace).UnixMilli()

	result := MediaGCResult{}
	err := l.iterMediaByState(ctx, MediaStateTombstoned, func(m MediaObject) error {
		if m.TombstonedAtMs > cutoff {
			result.Skipped++
			return nil
		}
		if err := l.BlobStore.Delete(ctx, m.BlobKey); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrBlobNotFound) {
			slog.Default().Warn("media-gc-delete: blob delete failed", "media_id", m.ID, "error", err)
			return nil
		}
		if err := l.MediaStore.Delete(ctx, m.ID); err != nil {
			slog.Default().Warn("media-gc-delete: metadata delete failed", "media_id", m.ID, "error", err)
			return nil
		}
		result.Deleted++
		return nil
	})
	if err != nil {
		return result, err
	}
	return result, nil
}

// PromoteReferencedMedia is called by the publish path after a manifest is
// committed: any pending media now referenced is transitioned to active.
// Idempotent.
func (l *KB) PromoteReferencedMedia(ctx context.Context, kbID string, mediaIDs []string) error {
	if l.MediaStore == nil || len(mediaIDs) == 0 {
		return nil
	}
	for _, id := range mediaIDs {
		m, err := l.MediaStore.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ErrMediaNotFound) {
				continue
			}
			return err
		}
		if m.KBID != kbID || m.State != MediaStatePending {
			continue
		}
		if err := l.MediaStore.UpdateState(ctx, id, MediaStateActive, 0); err != nil {
			return err
		}
	}
	return nil
}

// liveMediaRefsForKB returns the set of media_ids referenced by the current
// manifest for kbID. Older manifest versions are not walked.
func (l *KB) liveMediaRefsForKB(ctx context.Context, kbID string) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	if l.ManifestStore == nil {
		return out, nil
	}
	doc, err := l.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, ErrManifestNotFound) {
			return out, nil
		}
		return nil, err
	}
	for _, shard := range doc.Manifest.Shards {
		for _, id := range shard.MediaIDs {
			out[id] = struct{}{}
		}
	}
	return out, nil
}
