package kb

import (
	"context"
	"encoding/json"
	"fmt"
)

func (w *DocumentChunkedWorker) handleGraphExtract(ctx context.Context, event *KBEvent, payload DocumentChunkedPayload, embedded []EmbeddedDocument) (WorkerResult, error) {
	if w.KB.GraphBuilder == nil {
		return WorkerResult{}, ErrGraphUnavailable
	}
	graphDocs := make([]Document, 0, len(payload.Documents))
	for _, doc := range payload.Documents {
		graphDocs = append(graphDocs, doc)
	}
	graphResult, err := w.KB.GraphBuilder.Build(ctx, graphDocs)
	if err != nil {
		return WorkerResult{}, fmt.Errorf("build graph: %w", err)
	}
	nextPayload, _ := json.Marshal(DocumentGraphExtractedPayload{
		KBID:          payload.KBID,
		DocumentCount: payload.DocumentCount,
		Documents:     embedded,
		FileResults:   payload.FileResults,
		GraphResult:   graphResult,
		Options:       payload.Options,
		SourceEventID: payload.SourceEventID,
	})
	return WorkerResult{FollowUps: []KBEvent{newChildPendingEvent(event, EventDocumentGraphExtracted, "document.graph_extracted/v1", event.EventID+"|document.graph_extracted", nextPayload)}}, nil
}
