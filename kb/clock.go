package kb

import (
	"sync"
	"time"
)

// Clock returns the current time. Production code uses RealClock; tests and
// the simulation harness substitute FakeClock to drive time deterministically.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

// RealClock is the default Clock. It returns time.Now().UTC().
var RealClock Clock = realClock{}

// FakeClock is a deterministic Clock for tests and simulation. It never moves
// on its own; callers drive it forward with Advance or Set.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock returns a FakeClock initialised to t. If t is zero, the unix
// epoch is used.
func NewFakeClock(t time.Time) *FakeClock {
	if t.IsZero() {
		t = time.Unix(0, 0).UTC()
	}
	return &FakeClock{now: t.UTC()}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d. Negative durations are allowed for
// tests that need to simulate a clock that jumped backward.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set jumps the clock to t.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t.UTC()
}

// nowFrom returns c.Now() if c is non-nil, otherwise RealClock.Now().
// Keeps optional Clock fields ergonomic at call sites.
func nowFrom(c Clock) time.Time {
	if c == nil {
		return RealClock.Now()
	}
	return c.Now()
}
