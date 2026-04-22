package scenarios

import (
	"errors"

	"github.com/mikills/minnow/sim"
)

// HighDownloadFailure wipes the cache and then runs queries under a heavy
// download failure rate. Every query must either succeed with non-empty
// results or surface sim.ErrInjected. A silent success with zero results
// would indicate the read path masked an injected fault, which is a bug.
func HighDownloadFailure(h *sim.Harness) {
	const kbID = "download-faults"
	docs := h.GenerateDocs(kbID, 20)
	if err := h.Ingest(kbID, docs); err != nil {
		h.Fatalf("seed ingest (seed=%d): %v", h.Seed(), err)
	}
	h.RecordManifestVersion(kbID)

	probe, err := h.Embed(h.Ctx(), docs[0].Text)
	if err != nil {
		h.Fatalf("embed (seed=%d): %v", h.Seed(), err)
	}
	if _, err := h.Query(kbID, probe, 5); err != nil {
		h.Fatalf("warm query (seed=%d): %v", h.Seed(), err)
	}

	h.SetBlobFaults(sim.BlobFaults{DownloadFailRate: 0.5})
	defer h.SetBlobFaults(sim.BlobFaults{})

	if err := h.WipeCache(); err != nil {
		h.Fatalf("wipe cache (seed=%d): %v", h.Seed(), err)
	}

	ok, injected := 0, 0
	for i := 0; i < 20; i++ {
		results, err := h.Query(kbID, probe, 5)
		if err == nil {
			if len(results) == 0 {
				h.Errorf("query returned empty results with %d docs present (seed=%d)",
					len(docs), h.Seed())
			}
			ok++
			continue
		}
		if errors.Is(err, sim.ErrInjected) {
			injected++
			continue
		}
		h.Errorf("unexpected query error (seed=%d): %v", h.Seed(), err)
	}
	if ok == 0 && injected == 0 {
		h.Fatalf("neither success nor injected error observed (seed=%d)", h.Seed())
	}
}
