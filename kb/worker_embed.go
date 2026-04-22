package kb

import (
	"encoding/json"
)

func (w *DocumentChunkedWorker) handleEmbed(event *KBEvent, payload DocumentChunkedPayload, embedded []EmbeddedDocument) WorkerResult {
	nextPayload, _ := json.Marshal(DocumentEmbeddedPayload{
		KBID:          payload.KBID,
		DocumentCount: payload.DocumentCount,
		Documents:     embedded,
		FileResults:   payload.FileResults,
		Options:       payload.Options,
		SourceEventID: payload.SourceEventID,
	})
	return WorkerResult{FollowUps: []KBEvent{w.KB.newChildPendingEvent(event, EventDocumentEmbedded, "document.embedded/v1", event.EventID+"|document.embedded", nextPayload)}}
}
