package scenarios

import (
	"errors"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

// DimMismatchQuery ingests with the harness's default embedder and then
// queries with a vector of an obviously wrong dimension. The query must
// surface kb.ErrEmbeddingDimensionMismatch, not a generic wrapped error and
// not a silent empty-result response (either would indicate the shape check
// was bypassed).
func DimMismatchQuery(h *sim.Harness) {
	const kbID = "dim-mismatch"
	docs := h.GenerateDocs(kbID, 5)
	if err := h.Ingest(kbID, docs); err != nil {
		h.Fatalf("ingest (seed=%d): %v", h.Seed(), err)
	}
	h.RecordManifestVersion(kbID)

	// Default harness embedder is 32-dim; 1024 guarantees a mismatch.
	badVec := make([]float32, 1024)
	for i := range badVec {
		badVec[i] = 0.1
	}
	_, err := h.Query(kbID, badVec, 5)
	if err == nil {
		h.Errorf("expected error on dim mismatch, got nil (seed=%d)", h.Seed())
		return
	}
	if !errors.Is(err, kb.ErrEmbeddingDimensionMismatch) {
		h.Errorf("expected ErrEmbeddingDimensionMismatch, got %v (seed=%d)", err, h.Seed())
	}
}
