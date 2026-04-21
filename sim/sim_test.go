package sim

import (
	"fmt"
	"testing"
)

// TestSim exercises each scenario under a fixed set of seeds. Failures print
// the seed so a reviewer can rerun with -run to reproduce.
func TestSim(t *testing.T) {
	seeds := []int64{1, 2, 3, 7, 42, 100}

	scenarios := map[string]Scenario{
		"warm_vs_cold":     WarmVsCold,
		"flaky_blob_ingest": FlakyBlobIngest,
	}

	invariants := []Invariant{
		ManifestMonotonic(),
		NoDocLoss(50),
	}

	for name, scenario := range scenarios {
		name, scenario := name, scenario
		for _, seed := range seeds {
			seed := seed
			t.Run(fmt.Sprintf("%s/seed_%d", name, seed), func(t *testing.T) {
				h := New(t, WithSeed(seed), WithInvariants(invariants...))
				scenario(h)
				h.AssertInvariants()
			})
		}
	}
}
