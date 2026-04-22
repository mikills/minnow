package kb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type DocumentPublishWorker struct {
	KB        *KB
	ID        string
	KindValue EventKind
}

func (w *DocumentPublishWorker) Kind() EventKind  { return w.KindValue }
func (w *DocumentPublishWorker) WorkerID() string { return w.ID }

func (w *DocumentPublishWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	if event == nil {
		return WorkerResult{}, errors.New("publish worker: nil event")
	}
	if event.Kind != w.KindValue {
		return WorkerResult{}, fmt.Errorf("publish worker: event kind %q does not match worker kind %q",
			event.Kind, w.KindValue)
	}
	var (
		kbID          string
		docs          []EmbeddedDocument
		graphResult   *GraphBuildResult
		fileResults   []FileIngestResult
		documentCount int
		opts          UpsertDocsOptions
		sourceEventID string
	)
	switch event.Kind {
	case EventDocumentEmbedded:
		if event.PayloadSchema != "" && event.PayloadSchema != "document.embedded/v1" {
			return WorkerResult{}, fmt.Errorf("publish worker: expected document.embedded/v1 schema, got %q", event.PayloadSchema)
		}
		var payload DocumentEmbeddedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
		}
		kbID, docs, fileResults, documentCount, opts, sourceEventID = payload.KBID, payload.Documents, payload.FileResults, payload.DocumentCount, payload.Options, payload.SourceEventID
	case EventDocumentGraphExtracted:
		if event.PayloadSchema != "" && event.PayloadSchema != "document.graph_extracted/v1" {
			return WorkerResult{}, fmt.Errorf("publish worker: expected document.graph_extracted/v1 schema, got %q", event.PayloadSchema)
		}
		var payload DocumentGraphExtractedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
		}
		kbID, docs, fileResults, documentCount, opts, sourceEventID, graphResult = payload.KBID, payload.Documents, payload.FileResults, payload.DocumentCount, payload.Options, payload.SourceEventID, payload.GraphResult
	default:
		return WorkerResult{}, fmt.Errorf("publish worker: unsupported kind %s", event.Kind)
	}
	plainDocs := make([]Document, 0, len(docs))
	for _, doc := range docs {
		plainDocs = append(plainDocs, Document{ID: doc.ID, Text: doc.Text, MediaIDs: doc.MediaIDs, MediaRefs: doc.MediaRefs, Metadata: doc.Metadata})
	}
	if err := w.KB.ValidateDocumentReferences(ctx, kbID, plainDocs); err != nil {
		return WorkerResult{}, err
	}
	if err := w.KB.PublishPreparedDocs(ctx, kbID, docs, graphResult, opts); err != nil {
		return WorkerResult{}, err
	}
	mediaIDs := collectMediaIDs(plainDocs)
	pubPayload, _ := json.Marshal(KBPublishedPayload{KBID: kbID, DocumentCount: documentCount, ChunkCount: len(plainDocs), MediaIDs: mediaIDs, FileResults: fileResults, SourceEventID: sourceEventID})
	pub := w.KB.newChildPendingEvent(event, EventKBPublished, "kb.published/v1", sourceEventID+"|kb.published", pubPayload)
	return WorkerResult{FollowUps: []KBEvent{pub}, Commit: func(ctx context.Context) error {
		if err := w.KB.PromoteReferencedMedia(ctx, kbID, mediaIDs); err != nil {
			return fmt.Errorf("promote media: %w", err)
		}
		return nil
	}}, nil
}
