package scenarios

import (
	"fmt"
	"sync"

	"github.com/mikills/minnow/sim"
)

// ConcurrentManyWriters exercises write lease and manifest CAS behavior with
// more than two writers. Any acknowledged write is tracked by the harness and
// must remain queryable through the no-doc-loss invariant.
func ConcurrentManyWriters(h *sim.Harness) {
	const kbID = "concurrent-many-writers"
	const writers = 4
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for writer := range writers {
		wg.Add(1)
		startConcurrentManyWriter(h, &wg, results, kbID, writer)
	}
	wg.Wait()
	close(results)

	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
			continue
		}
		if !isExpectedCASConflict(err) {
			h.Errorf("unexpected writer error (seed=%d): %v", h.Seed(), err)
		}
	}
	if succeeded == 0 {
		h.Fatalf("all concurrent writers failed (seed=%d)", h.Seed())
	}
	h.RecordManifestVersion(kbID)
}

func startConcurrentManyWriter(h *sim.Harness, wg *sync.WaitGroup, results chan<- error, kbID string, writer int) {
	go func() {
		docs := h.GenerateDocs(fmt.Sprintf("%s-w%d", kbID, writer), 12)
		results <- h.Ingest(kbID, docs)
		wg.Done()
	}()
}
