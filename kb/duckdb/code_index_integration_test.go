package duckdb

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mikills/minnow/kb"
)

func TestCodeIndex(t *testing.T) {
	t.Run("incremental", func(t *testing.T) {
		ctx := context.Background()
		kbID := "code-index-incremental"
		embedder := &countingCodeEmbedder{base: newFixtureEmbedder(16)}
		h := kb.NewTestHarness(t, kbID).WithEmbedder(embedder).Setup()
		t.Cleanup(h.Cleanup)
		registerFormatOnHarness(t, h)

		root := t.TempDir()
		writeCodeTestFile(t, filepath.Join(root, "main.go"), "package main\nfunc Alpha() string { return \"alpha\" }\n")
		writeCodeTestFile(t, filepath.Join(root, "util.go"), "package main\nfunc Beta() string { return \"beta\" }\n")
		runCodeGit(t, root, "init")
		runCodeGit(t, root, "add", ".")

		first, err := h.KB().IndexCodebase(ctx, kb.CodeIndexOptions{KBID: kbID, IndexKey: "default", Description: "Primary Minnow codebase", Root: root, IncludeUntracked: true})
		require.NoError(t, err)
		require.Equal(t, "default", first.IndexKey)
		require.Equal(t, "Primary Minnow codebase", first.Description)
		require.Equal(t, 2, first.IndexedFiles)
		require.Equal(t, first.ChunksIndexed, embedder.Calls())
		firstCalls := embedder.Calls()
		registry, err := kb.LoadCodebaseIndexRegistry(root)
		require.NoError(t, err)
		require.Equal(t, kbID, registry.Indexes["default"].KBID)
		require.Equal(t, "Primary Minnow codebase", registry.Indexes["default"].Description)

		second, err := h.KB().IndexCodebase(ctx, kb.CodeIndexOptions{IndexKey: "default", Root: root})
		require.NoError(t, err)
		require.Equal(t, kbID, second.KBID)
		require.Equal(t, 0, second.IndexedFiles)
		require.Equal(t, firstCalls, embedder.Calls())

		writeCodeTestFile(t, filepath.Join(root, "util.go"), "package main\nfunc Beta() string { return \"changed\" }\n")
		third, err := h.KB().IndexCodebase(ctx, kb.CodeIndexOptions{KBID: kbID, Root: root, IncludeUntracked: true})
		require.NoError(t, err)
		require.Equal(t, 1, third.IndexedFiles)
		require.Greater(t, embedder.Calls(), firstCalls)

		results, err := h.KB().SearchCode(ctx, kbID, "changed beta", kb.CodeSearchOptions{TopK: 5, Path: "util.go", Language: "go"})
		require.NoError(t, err)
		require.NotEmpty(t, results)
		require.Equal(t, "util.go", results[0].Path)
		require.Equal(t, "go", results[0].Language)
	})
}

type countingCodeEmbedder struct {
	base  kb.Embedder
	calls int
}

func (e *countingCodeEmbedder) Embed(ctx context.Context, input string) ([]float32, error) {
	e.calls++
	return e.base.Embed(ctx, input)
}

func (e *countingCodeEmbedder) Calls() int { return e.calls }

func writeCodeTestFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func runCodeGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}
