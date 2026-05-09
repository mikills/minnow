package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/lease"
	"github.com/stretchr/testify/require"
)

func TestScheduler(t *testing.T) {
	t.Run("run once reports success", func(t *testing.T) {
		s := New(lease.NewInMemoryManager(), time.Minute, nil, nil)
		called := false
		require.NoError(t, s.Register("job", "* * * * *", func(context.Context) error {
			called = true
			return nil
		}))

		outcome, err := s.RunOnce(context.Background(), "job")

		require.NoError(t, err)
		require.Equal(t, OutcomeSuccess, outcome)
		require.True(t, called)
	})
}
