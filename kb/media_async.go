package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"strings"
	"time"
)

// MediaUploadWorkerVisibilityTimeout is the recommended claim window for
// MediaUploadWorker. Exposed as a tuneable constant so operators can raise
// it for slow blob backends.
const MediaUploadWorkerVisibilityTimeout = 5 * time.Minute

type MediaUploadPayload struct {
	KBID           string         `json:"kb_id"`
	MediaID        string         `json:"media_id"`
	StagedBlobKey  string         `json:"staged_blob_key"`
	Filename       string         `json:"filename"`
	ContentType    string         `json:"content_type"`
	SizeBytes      int64          `json:"size_bytes"`
	Checksum       string         `json:"checksum"`
	Source         string         `json:"source,omitempty"`
	Title          string         `json:"title,omitempty"`
	Tags           []string       `json:"tags,omitempty"`
	UploadedBy     string         `json:"uploaded_by,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	SourceEventID  string         `json:"source_event_id,omitempty"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

type MediaUploadedPayload struct {
	KBID          string `json:"kb_id"`
	MediaID       string `json:"media_id"`
	Filename      string `json:"filename"`
	ContentType   string `json:"content_type"`
	SizeBytes     int64  `json:"size_bytes"`
	Checksum      string `json:"checksum"`
	SourceEventID string `json:"source_event_id,omitempty"`
}

func mediaStagingBlobKey(kbID, eventID, filename string) string {
	return path.Join("kbs", kbID, "media-staging", eventID, filename)
}

func (l *KB) AppendMediaUploadDetailed(ctx context.Context, input MediaUploadInput, maxBytes int64, idempotencyKey, correlationID string) (string, string, error) {
	if l.EventStore == nil {
		return "", "", errors.New("media: EventStore not configured")
	}
	if l.BlobStore == nil {
		return "", "", errors.New("media: BlobStore not configured")
	}
	if strings.TrimSpace(input.KBID) == "" {
		return "", "", errors.New("media: kb_id required")
	}
	if input.Body == nil {
		return "", "", errors.New("media: body required")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxUploadBytes
	}
	cleanName, err := SanitizeMediaFilename(input.Filename)
	if err != nil {
		return "", "", err
	}
	ct := normaliseContentType(input.ContentType)
	allowList := input.AllowedContentTypes
	if len(allowList) == 0 {
		allowList = l.MediaContentTypeAllowlist
	}
	if len(allowList) > 0 && !isContentTypeAllowed(ct, allowList) {
		return "", "", fmt.Errorf("media: content type %q not allowed", ct)
	}
	if idempotencyKey == "" {
		idempotencyKey = l.NewULIDLike("idem")
	}
	if correlationID == "" {
		correlationID = l.NewULIDLike("corr")
	}
	tmp, err := writeTempCapped(input.Body, maxBytes)
	if err != nil {
		return "", "", err
	}
	defer tmp.cleanup()
	if tmp.size == 0 {
		return "", "", errors.New("media: empty upload rejected")
	}
	eventID := l.NewULIDLike("evt")
	mediaID := l.newMediaID()
	stagedBlobKey := mediaStagingBlobKey(input.KBID, eventID, cleanName)
	if _, err := l.BlobStore.UploadIfMatch(ctx, stagedBlobKey, tmp.path, ""); err != nil {
		return "", "", fmt.Errorf("media: stage upload: %w", err)
	}
	payloadBytes, err := json.Marshal(MediaUploadPayload{
		KBID:           input.KBID,
		MediaID:        mediaID,
		StagedBlobKey:  stagedBlobKey,
		Filename:       cleanName,
		ContentType:    ct,
		SizeBytes:      tmp.size,
		Checksum:       tmp.sha256,
		Source:         input.Source,
		Title:          input.Title,
		Tags:           input.Tags,
		UploadedBy:     input.UploadedBy,
		Metadata:       input.Metadata,
		SourceEventID:  eventID,
		CorrelationID:  correlationID,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "marshal payload failed")
		return "", "", fmt.Errorf("media: marshal payload: %w", err)
	}
	event := l.newRootPendingEvent(EventMediaUpload, input.KBID, "media.upload/v1", correlationID, idempotencyKey, payloadBytes)
	if err := l.EventStore.Append(ctx, event); err != nil {
		if errors.Is(err, ErrEventDuplicateKey) {
			existing, findErr := l.EventStore.FindByIdempotency(ctx, EventMediaUpload, input.KBID, idempotencyKey)
			if findErr != nil {
				return "", "", findErr
			}
			if existing != nil {
				bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "duplicate idempotency key")
				return existing.EventID, idempotencyKey, nil
			}
		}
		bestEffortDeleteStagedBlob(ctx, l.BlobStore, stagedBlobKey, "event append failed")
		return "", "", err
	}
	return event.EventID, idempotencyKey, nil
}

// bestEffortDeleteStagedBlob removes a staged blob, treating NotFound as
// success and logging any other error. Cascade-cleanup paths use this so a
// failed delete does not mask the primary error, but operators still see the
// potential leak in logs.
func bestEffortDeleteStagedBlob(ctx context.Context, store BlobStore, blobKey, reason string) {
	if store == nil || blobKey == "" {
		return
	}
	err := store.Delete(ctx, blobKey)
	if err == nil || errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
		return
	}
	slog.Default().Warn("media: staged blob cleanup failed",
		"blob_key", blobKey, "reason", reason, "error", err)
}

type MediaUploadWorker struct {
	KB *KB
	ID string
}

func (w *MediaUploadWorker) Kind() EventKind  { return EventMediaUpload }
func (w *MediaUploadWorker) WorkerID() string { return w.ID }

func (w *MediaUploadWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	if w.KB.MediaStore == nil || w.KB.BlobStore == nil {
		return WorkerResult{}, errors.New("media worker: media subsystem not configured")
	}
	var payload MediaUploadPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
	}
	if _, err := w.KB.MediaStore.Get(ctx, payload.MediaID); err != nil {
		if !errors.Is(err, ErrMediaNotFound) {
			return WorkerResult{}, err
		}
		rec := MediaObject{ID: payload.MediaID, KBID: payload.KBID, Filename: payload.Filename, ContentType: payload.ContentType, SizeBytes: payload.SizeBytes, Checksum: payload.Checksum, Source: payload.Source, Title: payload.Title, Tags: payload.Tags, UploadedBy: payload.UploadedBy, Metadata: payload.Metadata, State: MediaStatePending, IdempotencyKey: payload.IdempotencyKey}
		if err := w.KB.persistStagedMediaObject(ctx, payload.StagedBlobKey, rec); err != nil {
			return WorkerResult{}, err
		}
	}
	mediaUploadedPayload, err := json.Marshal(MediaUploadedPayload{KBID: payload.KBID, MediaID: payload.MediaID, Filename: payload.Filename, ContentType: payload.ContentType, SizeBytes: payload.SizeBytes, Checksum: payload.Checksum, SourceEventID: event.EventID})
	if err != nil {
		return WorkerResult{}, fmt.Errorf("marshal media.uploaded: %w", err)
	}
	child := w.KB.newChildPendingEvent(event, EventMediaUploaded, "media.uploaded/v1", event.EventID+"|media.uploaded", mediaUploadedPayload)
	return WorkerResult{FollowUps: []KBEvent{child}, Commit: func(ctx context.Context) error {
		if err := w.KB.BlobStore.Delete(ctx, payload.StagedBlobKey); err != nil && !errors.Is(err, ErrBlobNotFound) && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("media: delete staged blob %q: %w", payload.StagedBlobKey, err)
		}
		return nil
	}}, nil
}

// mediaSweepPageSize bounds each ListUnfinishedBefore call so the sweep
// does not materialise a large backlog in memory.
const mediaSweepPageSize = 500

func (l *KB) SweepAbortedMediaUploads(ctx context.Context, now time.Time) (int, error) {
	if l.EventStore == nil || l.BlobStore == nil {
		return 0, nil
	}
	if now.IsZero() {
		now = l.Clock.Now()
	}
	cutoff := now.Add(-l.MediaGCConfigOrDefault().UploadCompletion)
	count := 0
	var lastErr error
	token := ""
	for {
		events, next, err := l.EventStore.ListUnfinishedBefore(ctx, EventMediaUpload, cutoff, token, mediaSweepPageSize)
		if err != nil {
			return count, err
		}
		for _, event := range events {
			child, err := l.EventStore.FindByCausation(ctx, EventMediaUploaded, event.EventID)
			if err != nil {
				slog.Default().Warn("media-sweep: FindByCausation failed",
					"event_id", event.EventID, "error", err)
				lastErr = err
				continue
			}
			if child != nil {
				continue
			}
			var payload MediaUploadPayload
			if err := json.Unmarshal(event.Payload, &payload); err == nil {
				bestEffortDeleteStagedBlob(ctx, l.BlobStore, payload.StagedBlobKey, "sweep aborted upload")
			} else {
				slog.Default().Warn("media-sweep: decode payload failed",
					"event_id", event.EventID, "error", err)
			}
			if err := l.EventStore.Fail(ctx, event.EventID, event.Attempt, "media upload completion timeout"); err != nil {
				if errors.Is(err, ErrEventStateChanged) {
					slog.Default().Info("media-sweep: event state changed since observation; another worker updated it",
						"event_id", event.EventID)
					continue
				}
				slog.Default().Warn("media-sweep: EventStore.Fail failed",
					"event_id", event.EventID, "error", err)
				lastErr = err
				continue
			}
			count++
		}
		if next == "" {
			return count, lastErr
		}
		token = next
	}
}
