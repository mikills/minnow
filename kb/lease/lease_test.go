package lease

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInMemoryManager(t *testing.T) {
	mgr := NewInMemoryManager()
	first, err := mgr.Acquire(context.Background(), "kb", time.Minute)
	require.NoError(t, err)
	_, err = mgr.Acquire(context.Background(), "kb", time.Minute)
	require.ErrorIs(t, err, ErrConflict)
	renewed, err := mgr.Renew(context.Background(), first, time.Minute)
	require.NoError(t, err)
	require.Equal(t, first.Token, renewed.Token)
	require.NoError(t, mgr.Release(context.Background(), renewed))
	_, err = mgr.Acquire(context.Background(), "kb", time.Minute)
	require.NoError(t, err)
}
