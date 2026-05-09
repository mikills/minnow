package media

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/mikills/minnow/kb/blobstore"
)

const (
	DefaultPendingTTL       = 24 * time.Hour
	DefaultTombstoneGrace   = 1 * time.Hour
	DefaultUploadCompletion = 1 * time.Hour
)

type GCConfig struct {
	PendingTTL       time.Duration
	TombstoneGrace   time.Duration
	UploadCompletion time.Duration
}

func (c GCConfig) Defaults() GCConfig {
	if c.PendingTTL <= 0 {
		c.PendingTTL = DefaultPendingTTL
	}
	if c.TombstoneGrace <= 0 {
		c.TombstoneGrace = DefaultTombstoneGrace
	}
	if c.UploadCompletion <= 0 {
		c.UploadCompletion = DefaultUploadCompletion
	}
	return c
}

type GCResult struct {
	MarkedTombstone int
	Deleted         int
	Skipped         int
}

const logKeyError = "error"

type LiveRefs func(context.Context, string) (map[string]struct{}, error)

func SweepGCMark(
	ctx context.Context,
	store MediaStore,
	now time.Time,
	cfg GCConfig,
	liveRefs LiveRefs,
) (GCResult, error) {
	if store == nil {
		return GCResult{}, nil
	}
	cfg = cfg.Defaults()
	result := GCResult{}
	activeByKB := make(map[string]map[string]struct{})
	err := IterByState(ctx, store, MediaStatePending, func(m MediaObject) error {
		if m.CreatedAtUnixMs > now.Add(-cfg.PendingTTL).UnixMilli() {
			result.Skipped++
			return nil
		}
		active, ok := activeByKB[m.KBID]
		if !ok {
			refs, err := liveRefs(ctx, m.KBID)
			if err != nil {
				slog.Default().Warn("media-gc-mark: manifest fetch failed", "kb_id", m.KBID, logKeyError, err)
				result.Skipped++
				return nil
			}
			active = refs
			activeByKB[m.KBID] = active
		}
		if _, isReferenced := active[m.ID]; isReferenced {
			if err := store.UpdateState(ctx, m.ID, MediaStateActive, 0); err != nil {
				slog.Default().
					Warn("media-gc-mark: activate referenced media failed", "media_id", m.ID, logKeyError, err)
			}
			return nil
		}
		if err := store.UpdateState(ctx, m.ID, MediaStateTombstoned, now.UnixMilli()); err != nil {
			slog.Default().Warn("media-gc-mark: tombstone failed", "media_id", m.ID, logKeyError, err)
			return nil
		}
		result.MarkedTombstone++
		return nil
	})
	return result, err
}

func SweepGCDelete(ctx context.Context, store MediaStore, blobs interface {
	Delete(context.Context, string) error
}, now time.Time, cfg GCConfig) (GCResult, error) {
	if store == nil || blobs == nil {
		return GCResult{}, nil
	}
	cfg = cfg.Defaults()
	result := GCResult{}
	err := IterByState(ctx, store, MediaStateTombstoned, func(m MediaObject) error {
		if m.TombstonedAtMs > now.Add(-cfg.TombstoneGrace).UnixMilli() {
			result.Skipped++
			return nil
		}
		if err := blobs.Delete(ctx, m.BlobKey); err != nil && !errors.Is(err, os.ErrNotExist) &&
			!errors.Is(err, blobstore.ErrNotFound) {
			slog.Default().Warn("media-gc-delete: blob delete failed", "media_id", m.ID, logKeyError, err)
			return nil
		}
		if err := store.Delete(ctx, m.ID); err != nil {
			slog.Default().Warn("media-gc-delete: metadata delete failed", "media_id", m.ID, logKeyError, err)
			return nil
		}
		result.Deleted++
		return nil
	})
	return result, err
}

func PromoteReferenced(ctx context.Context, store MediaStore, kbID string, mediaIDs []string) error {
	if store == nil || len(mediaIDs) == 0 {
		return nil
	}
	for _, id := range mediaIDs {
		m, err := store.Get(ctx, id)
		if err != nil {
			if errors.Is(err, ErrMediaNotFound) {
				continue
			}
			return err
		}
		if m.KBID != kbID || m.State != MediaStatePending {
			continue
		}
		if err := store.UpdateState(ctx, id, MediaStateActive, 0); err != nil {
			return err
		}
	}
	return nil
}

func IterByState(ctx context.Context, store MediaStore, state MediaState, visit func(MediaObject) error) error {
	token := ""
	for {
		page, err := store.ListByState(ctx, "", state, token, MediaPageDefaultLimit)
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
