package kb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFakeClock(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("starts_at_provided_time", func(t *testing.T) {
		c := NewFakeClock(start)
		require.Equal(t, start, c.Now())
	})

	t.Run("advance_moves_forward", func(t *testing.T) {
		c := NewFakeClock(start)
		c.Advance(5 * time.Minute)
		require.Equal(t, start.Add(5*time.Minute), c.Now())
		c.Advance(1 * time.Hour)
		require.Equal(t, start.Add(5*time.Minute+time.Hour), c.Now())
	})

	t.Run("set_jumps_to_time", func(t *testing.T) {
		c := NewFakeClock(start)
		target := start.Add(24 * time.Hour)
		c.Set(target)
		require.Equal(t, target, c.Now())
	})

	t.Run("zero_initial_time_defaults_to_epoch", func(t *testing.T) {
		c := NewFakeClock(time.Time{})
		require.Equal(t, time.Unix(0, 0).UTC(), c.Now())
	})
}

func TestNowFromFallsBackToRealClock(t *testing.T) {
	t.Run("nil_clock_uses_real_clock", func(t *testing.T) {
		before := time.Now().UTC()
		got := nowFrom(nil)
		after := time.Now().UTC()
		require.False(t, got.Before(before.Add(-time.Second)))
		require.False(t, got.After(after.Add(time.Second)))
	})

	t.Run("fake_clock_is_honoured", func(t *testing.T) {
		target := time.Date(2030, 6, 15, 12, 0, 0, 0, time.UTC)
		c := NewFakeClock(target)
		require.Equal(t, target, nowFrom(c))
	})
}

func TestKBDefaultsToRealClock(t *testing.T) {
	kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir())
	require.NotNil(t, kb.Clock)
	require.Equal(t, RealClock, kb.Clock)
}

func TestKBWithClockOverride(t *testing.T) {
	fake := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	kb := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(), WithClock(fake))
	require.Same(t, fake, kb.Clock)
}
