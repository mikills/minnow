package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCron(t *testing.T) {
	t.Run("run due and remove", func(t *testing.T) {
		c := New()
		ran := make(chan struct{}, 1)
		require.NoError(t, c.Add("hourly", "0 * * * *", func() { ran <- struct{}{} }))

		c.RunDue(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
		select {
		case <-ran:
		case <-time.After(time.Second):
			require.Fail(t, "due job did not run")
		}

		c.Remove("hourly")
		c.RunDue(time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC))
		select {
		case <-ran:
			require.Fail(t, "removed job ran")
		case <-time.After(20 * time.Millisecond):
		}
	})

	t.Run("reject invalid expression", func(t *testing.T) {
		c := New()
		require.Error(t, c.Add("bad", "not a schedule", func() {}))
	})
}
