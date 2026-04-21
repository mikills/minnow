package scenarios

import (
	"errors"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

// ConcurrentWriters spawns two goroutines that upsert disjoint doc sets into
// the same KB. CAS should serialise them. At least one writer must succeed;
// the other may legitimately see ErrBlobVersionMismatch. Successful writers'
// docs must be queryable afterwards (checked by the no-doc-loss invariant).
func ConcurrentWriters(h *sim.Harness) {
	const kbID = "concurrent-writers"
	writerA := h.GenerateDocs(kbID+"-a", 20)
	writerB := h.GenerateDocs(kbID+"-b", 20)

	results := make(chan error, 2)
	go func() { results <- h.Ingest(kbID, writerA) }()
	go func() { results <- h.Ingest(kbID, writerB) }()

	var errs []error
	for i := 0; i < 2; i++ {
		errs = append(errs, <-results)
	}

	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		if !isExpectedCASConflict(err) {
			h.Errorf("unexpected writer error (seed=%d): %v", h.Seed(), err)
		}
	}
	if succeeded == 0 {
		h.Fatalf("both writers failed (seed=%d): %v", h.Seed(), errs)
	}
	h.RecordManifestVersion(kbID)
}

// isExpectedCASConflict returns true for error categories that naturally
// arise when two writers race on the manifest.
func isExpectedCASConflict(err error) bool {
	return errors.Is(err, kb.ErrBlobVersionMismatch)
}
