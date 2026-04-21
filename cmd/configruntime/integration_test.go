package configruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mikills/minnow/kb/config"
	"github.com/stretchr/testify/require"
)

// startLocalRuntime is a small helper that boots a runtime from a YAML body
// on a kernel-picked port, registers cleanup, and returns the base URL.
func startLocalRuntime(t *testing.T, yamlBody string) (*Runtime, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "minnow.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yamlBody), 0o644))

	cfg, err := config.Load(path)
	require.NoError(t, err)

	rt, err := Build(context.Background(), cfg, BuildOptions{Logger: quietLogger()})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rt.Stop(ctx)
	})

	require.NoError(t, rt.Start(context.Background()))
	return rt, "http://" + rt.App().Address()
}

// TestLocalModeIngestEndToEnd is the documented "fully local dev run"
// contract: minimal YAML => server => /rag/ingest => operation completes.
// Proves the in-memory event pipeline is wired without Mongo.
func TestLocalModeIngestEndToEnd(t *testing.T) {
	_, baseURL := startLocalRuntime(t, `
http:
  address: 127.0.0.1:0
  shutdown_timeout: 2s

embedder:
  provider: local
  local:
    dim: 16

scheduler:
  enabled: false

media:
  enabled: false
`)

	// Wait for the cache eviction loop / app startup to settle.
	time.Sleep(50 * time.Millisecond)

	body := map[string]any{
		"kb_id":         "kb-local-dev",
		"graph_enabled": false,
		"documents": []map[string]string{
			{"id": "doc-1", "text": "hello world from local mode"},
		},
		"chunk_size": 100,
	}
	resp, err := postJSON(baseURL+"/rag/ingest", body)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode, "ingest must be accepted in local mode")

	var accepted struct {
		EventID string `json:"event_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&accepted))
	require.NotEmpty(t, accepted.EventID, "in-memory event store must produce an event id")

	// Poll the operation endpoint. With workers running on the in-memory
	// store, the document should reach a terminal state quickly.
	deadline := time.Now().Add(5 * time.Second)
	var lastBody string
	for time.Now().Before(deadline) {
		opResp, err := http.Get(baseURL + "/rag/operations/" + accepted.EventID)
		if err == nil && opResp.StatusCode == http.StatusOK {
			b, _ := io.ReadAll(opResp.Body)
			opResp.Body.Close()
			lastBody = string(b)
			var payload map[string]any
			if json.Unmarshal(b, &payload) == nil {
				if terminal, ok := payload["terminal"].(map[string]any); ok {
					if kind, _ := terminal["kind"].(string); kind == "kb.published" {
						return
					}
					if kind, _ := terminal["kind"].(string); kind == "worker.failed" {
						t.Fatalf("operation reached worker.failed terminal state; body=%s", lastBody)
					}
				}
				if status, _ := payload["status"].(string); status == "dead" {
					t.Fatalf("operation reached dead state; body=%s", lastBody)
				}
			}
		}
		if opResp != nil {
			opResp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("operation did not reach terminal state within deadline; last body=%s", lastBody)
}

func TestLocalMode(t *testing.T) {
	t.Run("media_disabled_returns_503", func(t *testing.T) {
		_, baseURL := startLocalRuntime(t, `
http:
  address: 127.0.0.1:0
  shutdown_timeout: 2s

embedder:
  provider: local
  local:
    dim: 16

scheduler:
  enabled: false

media:
  enabled: false
`)

		resp, err := http.Get(baseURL + "/rag/media/list?kb_id=any")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode,
			"media.enabled=false must surface as 503 on list")

		getResp, err := http.Get(baseURL + "/rag/media/missing-id")
		require.NoError(t, err)
		defer getResp.Body.Close()
		require.Equal(t, http.StatusServiceUnavailable, getResp.StatusCode,
			"media.enabled=false must surface as 503 on get")
	})

	t.Run("no_media_upload_worker_when_media_disabled", func(t *testing.T) {
		disabled, _ := startLocalRuntime(t, `
http:
  address: 127.0.0.1:0
  shutdown_timeout: 2s
embedder:
  provider: local
  local:
    dim: 16
scheduler:
  enabled: false
media:
  enabled: false
`)
		enabled, _ := startLocalRuntime(t, `
http:
  address: 127.0.0.1:0
  shutdown_timeout: 2s
embedder:
  provider: local
  local:
    dim: 16
scheduler:
  enabled: false
media:
  enabled: true
`)
		require.Equal(t, len(disabled.WorkerPools())+1, len(enabled.WorkerPools()),
			"enabling media must add exactly one worker pool (media-upload)")
	})

	t.Run("media_enabled_serves_routes", func(t *testing.T) {
		_, baseURL := startLocalRuntime(t, `
http:
  address: 127.0.0.1:0
  shutdown_timeout: 2s

embedder:
  provider: local
  local:
    dim: 16

scheduler:
  enabled: false

media:
  enabled: true
  max_bytes: 1024
`)

		resp, err := http.Get(baseURL + "/rag/media/list?kb_id=any")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.NotEqual(t, http.StatusServiceUnavailable, resp.StatusCode,
			"with media.enabled=true, list route must not 503 (got %d)", resp.StatusCode)
	})
}

func postJSON(url string, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-KB-ID", fmt.Sprintf("%v", extractKBID(body)))
	return http.DefaultClient.Do(req)
}

// extractKBID pulls the kb_id from the body if present, so the X-KB-ID
// middleware in cmd/app.go has the routing key it expects.
func extractKBID(body any) string {
	m, ok := body.(map[string]any)
	if !ok {
		return ""
	}
	if v, ok := m["kb_id"].(string); ok {
		return v
	}
	return ""
}
