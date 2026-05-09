package metrics

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInMemAppSnapshot(t *testing.T) {
	m := NewInMemApp()
	m.RecordRequest("get", "/health", 200, 12)
	m.RecordEmbed("kb", 3, nil)
	m.RecordQuery("kb", 4, 2, 0.5, errors.New("boom"))
	m.RecordMediaUpload("kb", "image/png; charset=utf-8", 10, nil)

	snap := m.Snapshot()
	require.Equal(t, int64(1), snap.RouteStats["GET /health"].Count)
	require.Equal(t, int64(1), snap.EmbedStats["kb"].Count)
	require.Equal(t, int64(1), snap.QueryStats["kb"].ErrorCount)
	require.Equal(t, int64(10), snap.MediaUploadStats["kb|image/png"].TotalBytes)
	require.Len(t, snap.RecentRequests, 1)
}
