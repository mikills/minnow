package kb

import (
	"context"
	"fmt"
	"time"
)

// Default cron expressions for minnow maintenance jobs. Cadence is part of the
// job's contract. It is not configurable per job at runtime.
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

	if err := s.Register(ShardGCJobID, ShardGCJobExpr, l.runShardGCJob); err != nil {
		return err
	}
	return l.registerRemainingDefaultJobs(s)
}

func (l *KB) registerRemainingDefaultJobs(s *Scheduler) error {
	if err := l.registerEventJobs(s); err != nil {
		return err
	}
	if err := l.registerInboxJobs(s); err != nil {
		return err
	}
	if err := l.registerMediaJobs(s); err != nil {
		return err
	}
	return l.registerMediaUploadAbortJob(s)
}

func (l *KB) registerEventJobs(s *Scheduler) error {
	if l.EventStore == nil {
		return nil
	}
	return firstSchedulerJobErr(
		func() error { return s.Register(EventReaperJobID, EventReaperJobExpr, l.runEventReaperJob) },
		func() error { return s.Register(EventCleanupJobID, EventCleanupJobExpr, l.runEventCleanupJob) },
	)
}

func (l *KB) registerInboxJobs(s *Scheduler) error {
	if l.EventInbox == nil {
		return nil
	}
	return s.Register(InboxCleanupJobID, InboxCleanupJobExpr, l.runInboxCleanupJob)
}

func (l *KB) registerMediaJobs(s *Scheduler) error {
	if l.MediaStore == nil {
		return nil
	}
	return firstSchedulerJobErr(
		func() error { return s.Register(MediaGCMarkJobID, MediaGCMarkJobExpr, l.runMediaGCMarkJob) },
		func() error { return s.Register(MediaGCSweepJobID, MediaGCSweepJobExpr, l.runMediaGCSweepJob) },
	)
}

func (l *KB) registerMediaUploadAbortJob(s *Scheduler) error {
	if l.EventStore == nil || l.BlobStore == nil {
		return nil
	}
	return s.Register(MediaUploadAbortJobID, MediaUploadAbortJobExpr, l.runMediaUploadAbortJob)
}

func firstSchedulerJobErr(steps ...func() error) error {
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func (l *KB) runShardGCJob(ctx context.Context) error {
	_, err := l.SweepDelayedShardGC(ctx, time.Time{})
	return err
}

func (l *KB) runEventReaperJob(ctx context.Context) error {
	_, err := l.EventStore.Requeue(ctx, l.Clock.Now())
	return err
}

func (l *KB) runEventCleanupJob(ctx context.Context) error {
	_, err := l.EventStore.Cleanup(ctx, l.Clock.Now().Add(-EventRetention))
	return err
}

func (l *KB) runInboxCleanupJob(ctx context.Context) error {
	_, err := l.EventInbox.Cleanup(ctx, l.Clock.Now().Add(-InboxRetention))
	return err
}

func (l *KB) runMediaGCMarkJob(ctx context.Context) error {
	_, err := l.SweepMediaGCMark(ctx, time.Time{})
	return err
}

func (l *KB) runMediaGCSweepJob(ctx context.Context) error {
	_, err := l.SweepMediaGCDelete(ctx, time.Time{})
	return err
}

func (l *KB) runMediaUploadAbortJob(ctx context.Context) error {
	_, err := l.SweepAbortedMediaUploads(ctx, time.Time{})
	return err
}
