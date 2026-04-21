package kb

import (
	"context"
	"encoding/json"
	"fmt"
)

type DocumentChunkedWorker struct {
	KB *KB
	ID string
}

func (w *DocumentChunkedWorker) Kind() EventKind  { return EventDocumentChunked }
func (w *DocumentChunkedWorker) WorkerID() string { return w.ID }

func (w *DocumentChunkedWorker) Handle(ctx context.Context, event *KBEvent) (WorkerResult, error) {
	var payload DocumentChunkedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return WorkerResult{}, fmt.Errorf("decode payload: %w", err)
	}
	embedded := make([]EmbeddedDocument, 0, len(payload.Documents))
	for _, doc := range payload.Documents {
		vec, err := w.KB.Embed(ctx, doc.Text)
		if err != nil {
			return WorkerResult{}, fmt.Errorf("embed doc %q: %w", doc.ID, err)
		}
		embedded = append(embedded, EmbeddedDocument{
			ID:        doc.ID,
			Text:      doc.Text,
			MediaIDs:  doc.MediaIDs,
			MediaRefs: doc.MediaRefs,
			Metadata:  doc.Metadata,
			Embedding: vec,
		})
	}
	if payload.Options.GraphEnabled != nil && *payload.Options.GraphEnabled {
		return w.handleGraphExtract(ctx, event, payload, embedded)
	}
	return w.handleEmbed(event, payload, embedded), nil
}
