package kb

import (
	"context"
	"fmt"
	"time"
)

// Default cron expressions for minnow maintenance jobs. Cadence is part of the
// job's contract; it is not configurable per-job at runtime.
const (
	ShardGCJobID            = "shard-gc"
	ShardGCJobExpr          = "*/2 * * * *"
	EventReaperJobID        = "event-reaper"
	EventReaperJobExpr      = "* * * * *"
	InboxCleanupJobID       = "inbox-cleanup"
	InboxCleanupJobExpr     = "0 * * * *"
	MediaGCMarkJobID        = "media-gc-mark"
	MediaGCMarkJobExpr      = "*/5 * * * *"
	MediaGCSweepJobID       = "media-gc-sweep"
	MediaGCSweepJobExpr     = "*/15 * * * *"
	MediaUploadAbortJobID   = "media-upload-abort"
	MediaUploadAbortJobExpr = "*/5 * * * *"
	EventCleanupJobID       = "event-cleanup"
	EventCleanupJobExpr     = "0 */6 * * *"
)

// InboxRetention controls how long inbox rows are kept before cleanup.
const InboxRetention = 7 * 24 * time.Hour

// EventRetention controls how long terminal events (done, dead) are kept
// before the event-cleanup sweep deletes them.
const EventRetention = 30 * 24 * time.Hour

// RegisterDefaultJobs registers the periodic maintenance jobs that the KB
// owns directly. Returns the first registration error.
func (l *KB) RegisterDefaultJobs(s *Scheduler) error {
	if s == nil {
		return fmt.Errorf("scheduler: nil")
	}

	if err := s.Register(ShardGCJobID, ShardGCJobExpr, func(ctx context.Context) error {
		_, err := l.SweepDelayedShardGC(ctx, time.Time{})
		return err
	}); err != nil {
		return err
	}

	if l.EventStore != nil {
		if err := s.Register(EventReaperJobID, EventReaperJobExpr, func(ctx context.Context) error {
			_, err := l.EventStore.Requeue(ctx, l.Clock.Now())
			return err
		}); err != nil {
			return err
		}
		if err := s.Register(EventCleanupJobID, EventCleanupJobExpr, func(ctx context.Context) error {
			_, err := l.EventStore.Cleanup(ctx, l.Clock.Now().Add(-EventRetention))
			return err
		}); err != nil {
			return err
		}
	}

	if l.EventInbox != nil {
		if err := s.Register(InboxCleanupJobID, InboxCleanupJobExpr, func(ctx context.Context) error {
			_, err := l.EventInbox.Cleanup(ctx, l.Clock.Now().Add(-InboxRetention))
			return err
		}); err != nil {
			return err
		}
	}

	if l.MediaStore != nil {
		if err := s.Register(MediaGCMarkJobID, MediaGCMarkJobExpr, func(ctx context.Context) error {
			_, err := l.SweepMediaGCMark(ctx, time.Time{})
			return err
		}); err != nil {
			return err
		}
		if err := s.Register(MediaGCSweepJobID, MediaGCSweepJobExpr, func(ctx context.Context) error {
			_, err := l.SweepMediaGCDelete(ctx, time.Time{})
			return err
		}); err != nil {
			return err
		}
	}

	if l.EventStore != nil && l.BlobStore != nil {
		if err := s.Register(MediaUploadAbortJobID, MediaUploadAbortJobExpr, func(ctx context.Context) error {
			_, err := l.SweepAbortedMediaUploads(ctx, time.Time{})
			return err
		}); err != nil {
			return err
		}
	}

	return nil
}
