package scenarios

import (
	"errors"
	"fmt"

	"github.com/mikills/minnow/sim"
)

// FlakyBlobIngest injects a non-zero upload failure rate and asserts that
// ingest either succeeds after retry or surfaces sim.ErrInjected. Silent
// data loss (success reported, docs missing) is what the no-doc-loss
// invariant catches after the scenario returns.
func FlakyBlobIngest(h *sim.Harness) {
	const kbID = "flaky-blob"
	h.SetBlobFaults(sim.BlobFaults{UploadFailRate: 0.15})
	defer h.SetBlobFaults(sim.BlobFaults{})

	for batch := 0; batch < 5; batch++ {
		docs := h.GenerateDocs(fmt.Sprintf("%s-b%d", kbID, batch), 10)
		for i := range docs {
			docs[i].ID = fmt.Sprintf("%s-%d-%d", kbID, batch, i)
		}

		var lastErr error
		for attempt := 0; attempt < 10; attempt++ {
			lastErr = h.Ingest(kbID, docs)
			if lastErr == nil {
				break
			}
			if !errors.Is(lastErr, sim.ErrInjected) {
				h.Fatalf("unexpected non-injected error on batch %d attempt %d (seed=%d): %v",
					batch, attempt, h.Seed(), lastErr)
			}
		}
		if lastErr != nil {
			h.Fatalf("ingest exhausted retries on batch %d (seed=%d): %v",
				batch, h.Seed(), lastErr)
		}
		h.RecordManifestVersion(kbID)
	}
}
