package scenarios

import (
	"errors"
	"time"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

// AsyncDocumentPipeline runs the document.upsert -> chunked -> embedded ->
// publish worker chain through the event store and asserts the published doc is
// searchable. It also verifies duplicate idempotency keys collapse to one root
// command event.
func AsyncDocumentPipeline(h *sim.Harness) {
	const kbID = "async-document-pipeline"
	ctx := h.Ctx()
	docs := h.GenerateDocs(kbID, 3)
	payload := kb.DocumentUpsertPayload{KBID: kbID, Documents: docs, ChunkSize: 48}

	eventID, idem, err := h.KB().AppendDocumentUpsertDetailed(ctx, payload, "idem-async-docs", "corr-async-docs")
	if err != nil {
		h.Fatalf("append async upsert failed: %v", err)
	}
	duplicateID, _, err := h.KB().AppendDocumentUpsertDetailed(ctx, payload, idem, "corr-async-docs-duplicate")
	if err != nil {
		h.Fatalf("duplicate async upsert failed: %v", err)
	}
	if duplicateID != eventID {
		h.Fatalf("duplicate idempotency key created new event: got=%s want=%s", duplicateID, eventID)
	}

	runWorkerOnce(h, &kb.DocumentUpsertWorker{KB: h.KB(), ID: "upsert"})
	runWorkerOnce(h, &kb.DocumentChunkedWorker{KB: h.KB(), ID: "chunked"})
	runWorkerOnce(h, &kb.DocumentPublishWorker{KB: h.KB(), ID: "publish", KindValue: kb.EventDocumentEmbedded})

	probe, err := h.Embed(ctx, docs[0].Text)
	if err != nil {
		h.Fatalf("embed async probe failed: %v", err)
	}
	matches, err := h.Search(kbID, probe, 5)
	if err != nil {
		h.Fatalf("async published search failed: %v", err)
	}
	if len(matches) == 0 {
		h.Fatalf("async published docs were not searchable")
	}
	h.RecordManifestVersion(kbID)
}

func runWorkerOnce(h *sim.Harness, worker kb.Worker) {
	pool, err := kb.NewWorkerPool(
		worker,
		h.KB().EventStore,
		h.KB().EventInbox,
		kb.WorkerPoolConfig{Concurrency: 1, PollInterval: time.Millisecond, MaxAttempts: 3},
	)
	if err != nil {
		h.Fatalf("create worker pool for %s failed: %v", worker.Kind(), err)
	}
	_, err = pool.HandleOnce(h.Ctx())
	if err != nil && !errors.Is(err, kb.ErrEventNoneAvailable) {
		h.Fatalf("handle worker %s failed: %v", worker.Kind(), err)
	}
}
