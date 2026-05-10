package scenarios

import (
	"errors"
	"fmt"
	"math/rand"

	"github.com/mikills/minnow/sim"
)

// RandomOpsFuzz runs a seeded random sequence of operations (ingest, query,
// wipe cache, toggle fault rates) against a small pool of KBs and relies on
// the registered invariants to catch drift. Regression target: any arbitrary
// combination of operations that produces a state the individual scenarios
// don't exercise.
func RandomOpsFuzz(h *sim.Harness) {
	const (
		numKBs   = 3
		numOps   = 40
		batchLen = 8
	)

	// Use a dedicated RNG so we don't trample the harness's shared one.
	rng := rand.New(rand.NewSource(h.Seed() + 7919))

	kbIDs := make([]string, numKBs)
	for i := range kbIDs {
		kbIDs[i] = fmt.Sprintf("fuzz-kb-%d", i)
	}

	for step := range numOps {
		runRandomFuzzOp(h, rng, kbIDs, batchLen, step)
	}

	// Leave the store in a clean state for the invariant pass.
	h.SetBlobFaults(sim.BlobFaults{})
}

func runRandomFuzzOp(h *sim.Harness, rng *rand.Rand, kbIDs []string, batchLen, step int) {
	op := rng.Intn(100)
	kbID := kbIDs[rng.Intn(len(kbIDs))]
	switch {
	case op < 50:
		runRandomFuzzIngest(h, kbID, batchLen, step)
	case op < 85:
		runRandomFuzzQuery(h, kbID, step)
	case op < 92:
		runRandomFuzzWipeCache(h, step)
	case op < 96:
		h.SetBlobFaults(sim.BlobFaults{UploadFailRate: 0.1, DownloadFailRate: 0.1})
	default:
		h.SetBlobFaults(sim.BlobFaults{})
	}
}

func runRandomFuzzIngest(h *sim.Harness, kbID string, batchLen, step int) {
	docs := h.GenerateDocs(fmt.Sprintf("%s-s%d", kbID, step), batchLen)
	for i := range docs {
		docs[i].ID = fmt.Sprintf("%s-s%d-%d", kbID, step, i)
	}
	err := h.Ingest(kbID, docs)
	if err != nil && !errors.Is(err, sim.ErrInjected) {
		h.Errorf("fuzz ingest step %d (seed=%d, kb=%s): %v", step, h.Seed(), kbID, err)
		return
	}
	if err == nil {
		h.RecordManifestVersion(kbID)
	}
}

func runRandomFuzzQuery(h *sim.Harness, kbID string, step int) {
	vec := h.RandomVec(32)
	_, err := h.Search(kbID, vec, 5)
	if err != nil && !errors.Is(err, sim.ErrInjected) && !isTolerableConcurrentReadError(err) {
		h.Errorf("fuzz query step %d (seed=%d, kb=%s): %v", step, h.Seed(), kbID, err)
	}
}

func runRandomFuzzWipeCache(h *sim.Harness, step int) {
	if err := h.WipeCache(); err != nil {
		h.Errorf("fuzz wipe cache step %d (seed=%d): %v", step, h.Seed(), err)
	}
}
