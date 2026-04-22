package scenarios

import (
	"errors"
	"sync"

	kb "github.com/mikills/minnow/kb"
	"github.com/mikills/minnow/sim"
)

// ConcurrentIngestAndQuery runs a writer that ingests several batches while
// a reader runs many queries in parallel. The reader must never panic or
// receive a corrupt result; intermediate states (no manifest yet, partial
// results) are acceptable. Regression target: read path unsafety while a
// publish is swapping the manifest or shard files underneath.
func ConcurrentIngestAndQuery(h *sim.Harness) {
	const (
		kbID     = "concurrent-rw"
		batches  = 5
		batchLen = 10
		queries  = 40
	)

	// Pre-generate all docs and probe vectors on the main goroutine so the
	// harness's RNG isn't touched from two goroutines at once.
	ingestBatches := make([][]kb.Document, batches)
	for i := 0; i < batches; i++ {
		docs := h.GenerateDocs(kbID+"-pre", batchLen)
		for j := range docs {
			docs[j].ID = kbID + "-" + docs[j].ID
		}
		ingestBatches[i] = docs
	}
	probes := make([][]float32, queries)
	for i := range probes {
		probes[i] = h.RandomVec(32)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Writer.
	go func() {
		defer wg.Done()
		for _, batch := range ingestBatches {
			if err := h.Ingest(kbID, batch); err != nil {
				h.Errorf("concurrent ingest (seed=%d): %v", h.Seed(), err)
				return
			}
		}
	}()

	// Reader.
	go func() {
		defer wg.Done()
		for _, vec := range probes {
			_, err := h.Query(kbID, vec, 5)
			if err == nil {
				continue
			}
			// A pre-first-publish query legitimately surfaces
			// ErrKBUninitialized; anything else during the race is
			// suspicious and worth flagging.
			if !isTolerableConcurrentReadError(err) {
				h.Errorf("concurrent query (seed=%d): %v", h.Seed(), err)
			}
		}
	}()

	wg.Wait()
	h.RecordManifestVersion(kbID)
}

// isTolerableConcurrentReadError reports whether a read-path error is
// acceptable during a concurrent write. The main case is the reader racing
// the first publish: the manifest does not exist yet.
func isTolerableConcurrentReadError(err error) bool {
	if err == nil {
		return true
	}
	return errors.Is(err, kb.ErrKBUninitialized) ||
		errors.Is(err, kb.ErrManifestNotFound)
}
