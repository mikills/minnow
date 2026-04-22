package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

type DocumentUpsertWorker struct {
	KB *KB
	ID string
}

func (w *DocumentUpsertWorker) Kind() EventKind { return EventDocumentUpsert }

func (w *DocumentUpsertWorker) WorkerID() string { return w.ID }

func (w *DocumentUpsertWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	var payload DocumentUpsertPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
	}
	chunkedDocs, fileResults, _, stagedBlobKeys, err := w.normalizeDocuments(ctx, payload.KBID, payload.Documents, payload.FileSources, payload.ChunkSize)
	if err != nil {
		return WorkerResult{}, err
	}
	nextPayload, _ := json.Marshal(DocumentChunkedPayload{
		KBID:          payload.KBID,
		DocumentCount: len(payload.Documents) + successfulFileCount(fileResults),
		Documents:     chunkedDocs,
		FileResults:   fileResults,
		Options:       payload.Options,
		SourceEventID: event.EventID,
	})
	next := w.KB.newChildPendingEvent(event, EventDocumentChunked, "document.chunked/v1", event.EventID+"|document.chunked", nextPayload)
	return WorkerResult{FollowUps: []KBEvent{next}, Commit: func(ctx context.Context) error {
		var firstErr error
		for _, key := range stagedBlobKeys {
			err := w.KB.BlobStore.Delete(ctx, key)
			if err == nil || errors.Is(err, ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
				continue
			}
			slog.Default().Warn("ingest: staged blob delete failed",
				"blob_key", key, "error", err)
			if firstErr == nil {
				firstErr = fmt.Errorf("ingest: delete staged blob %q: %w", key, err)
			}
		}
		return firstErr
	}}, nil
}

func successfulFileCount(results []FileIngestResult) int {
	count := 0
	for _, result := range results {
		if result.Status == "succeeded" {
			count++
		}
	}
	return count
}
