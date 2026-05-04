package kb

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodeIndex(t *testing.T) {
	t.Run("chunks symbols", func(t *testing.T) {
		src := `package demo

func Alpha() string {
	return "alpha"
}

type Beta struct{}

func Gamma() string {
	return "gamma"
}
`
		chunks := chunkCodeText(src, "go", 80, 10)
		require.NotEmpty(t, chunks)
		require.Equal(t, "Alpha", chunks[0].Symbol)
		require.Equal(t, "function", chunks[0].Kind)
		require.GreaterOrEqual(t, chunks[0].StartLine, 3)
		require.GreaterOrEqual(t, chunks[0].EndLine, chunks[0].StartLine)

		foundBeta := false
		for _, chunk := range chunks {
			if chunk.Symbol == "Beta" && chunk.Kind == "type" {
				foundBeta = true
			}
		}
		require.True(t, foundBeta)
	})

	t.Run("scan respects ignores", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		writeTestFile(t, filepath.Join(root, "main.go"), "package main\nfunc main() {}\n")
		writeTestFile(t, filepath.Join(root, "node_modules", "dep.js"), "module.exports = {}\n")
		writeTestFile(t, filepath.Join(root, ".env"), "SECRET=1\n")
		writeTestFile(t, filepath.Join(root, "ignored.go"), "package ignored\n")
		writeTestFile(t, filepath.Join(root, ".gitignore"), "ignored.go\n")
		runGit(t, root, "init")
		runGit(t, root, "add", ".")

		files, skipped, err := scanCodebase(ctx, root, normalizeCodeIndexOptions(CodeIndexOptions{IncludeUntracked: true}))
		require.NoError(t, err)
		require.Greater(t, skipped, 0)
		require.Len(t, files, 1)
		require.Equal(t, "main.go", files[0].RelPath)
		require.Equal(t, "go", files[0].Language)
	})

	t.Run("defaults prefer source", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		writeTestFile(t, filepath.Join(root, "main.go"), "package main\nfunc main() {}\n")
		writeTestFile(t, filepath.Join(root, "README.md"), "# demo\n")
		writeTestFile(t, filepath.Join(root, "fixture.bin"), "not source\n")
		runGit(t, root, "init")
		runGit(t, root, "add", ".")

		files, _, err := scanCodebase(ctx, root, normalizeCodeIndexOptions(CodeIndexOptions{IncludeUntracked: true}))
		require.NoError(t, err)
		require.Len(t, files, 2)
		require.Equal(t, []string{"README.md", "main.go"}, []string{files[0].RelPath, files[1].RelPath})
	})

	t.Run("wildcard includes unknown text", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		writeTestFile(t, filepath.Join(root, "fixture.custom"), "custom text\n")
		runGit(t, root, "init")
		runGit(t, root, "add", ".")

		files, _, err := scanCodebase(ctx, root, normalizeCodeIndexOptions(CodeIndexOptions{Include: []string{"**/*"}, IncludeUntracked: true}))
		require.NoError(t, err)
		require.Len(t, files, 1)
		require.Equal(t, "fixture.custom", files[0].RelPath)
	})

	t.Run("wildcard skips binary content", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		writeTestFile(t, filepath.Join(root, "text.custom"), "custom text\n")
		require.NoError(t, os.WriteFile(filepath.Join(root, "image.custom"), []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0x00, 0x01}, 0o644))
		runGit(t, root, "init")
		runGit(t, root, "add", ".")

		files, skipped, err := scanCodebase(ctx, root, normalizeCodeIndexOptions(CodeIndexOptions{Include: []string{"**/*"}, IncludeUntracked: true}))
		require.NoError(t, err)
		require.GreaterOrEqual(t, skipped, 1)
		require.Len(t, files, 1)
		require.Equal(t, "text.custom", files[0].RelPath)
	})

	t.Run("recursive globs", func(t *testing.T) {
		require.True(t, matchesAnyCodePattern("main.go", []string{"**/*.go"}))
		require.True(t, matchesAnyCodePattern("src/main.go", []string{"src/**/*.go"}))
		require.True(t, matchesAnyCodePattern("generated/api/types.go", []string{"**/generated/**"}))
		require.True(t, matchesAnyCodePattern("packages/app/src/main.ts", []string{"packages/*/src/**/*.ts"}))
		require.True(t, matchesAnyCodePattern("django/contrib/admin/static/admin/js/vendor/jquery/jquery.min.js", []string{"**/vendor/**"}))
		require.True(t, matchesAnyCodePattern("django/contrib/admin/static/admin/js/vendor/jquery/jquery.min.js", []string{"**/*.min.js"}))
		require.True(t, matchesAnyCodePattern("tests/gis_tests/data/geometries.json", []string{"**/data/**"}))
		require.False(t, matchesAnyCodePattern("packages/app/test/main.ts", []string{"packages/*/src/**/*.ts"}))
	})

	t.Run("splits long lines", func(t *testing.T) {
		chunks := chunkCodeText(strings.Repeat("a", 3500), "json", 1000, 100)
		require.Len(t, chunks, 4)
		for _, chunk := range chunks {
			require.LessOrEqual(t, len(chunk.Text), 1000)
			require.Equal(t, 1, chunk.StartLine)
			require.Equal(t, 1, chunk.EndLine)
		}
	})

	t.Run("resource defaults", func(t *testing.T) {
		policy := codeIndexResourcePolicyFromOptions(CodeIndexOptions{})
		require.Equal(t, DefaultCodeEmbedBatchSize, policy.EmbedBatchSize)
		require.Equal(t, DefaultCodeMaxBatchBytes, policy.MaxBatchBytes)
		require.Equal(t, DefaultCodeThrottle, policy.Throttle)
		require.Equal(t, uint64(DefaultCodeMaxHeapBytes), policy.MaxHeapBytes)
		require.Equal(t, uint64(DefaultCodeMaxRSSBytes), policy.MaxRSSBytes)
		require.Equal(t, DefaultCodeLargeRepoFiles, policy.LargeRepoFiles)

		opts := normalizeCodeIndexOptions(CodeIndexOptions{})
		require.Equal(t, policy.EmbedBatchSize, opts.EmbedBatchSize)
		require.Equal(t, policy.MaxBatchBytes, opts.MaxBatchBytes)
	})

	t.Run("resource guard", func(t *testing.T) {
		err := CodeIndexResourcePolicy{MaxHeapBytes: 1}.Check(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "resource guard")
	})

	t.Run("resource batch end", func(t *testing.T) {
		policy := CodeIndexResourcePolicy{EmbedBatchSize: 3, MaxBatchBytes: 10}
		docs := []Document{{Text: "1234"}, {Text: "5678"}, {Text: "9012"}}
		require.Equal(t, 2, policy.BatchEnd(docs))
		require.Equal(t, 12, documentsTextBytes(docs))
	})

	t.Run("failed refresh keeps old chunks", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		writeTestFile(t, filepath.Join(root, "main.go"), "package main\nfunc main() {}\n")
		runGit(t, root, "init")
		runGit(t, root, "add", ".")

		format := &failingCodeIndexStreamFormat{err: errors.New("embed failed")}
		k := NewKB(&LocalBlobStore{Root: t.TempDir()}, t.TempDir(), WithArtifactFormat(format))
		oldManifest := codeIndexManifest{
			SchemaVersion: CodeIndexManifestSchema,
			KBID:          "code",
			Root:          root,
			RepoID:        codeRepoID(root),
			Files: map[string]codeIndexedFile{
				"main.go": {Path: "main.go", Hash: "old-hash", Language: "go", ChunkIDs: []string{"old-chunk"}},
			},
			Chunks: map[string]CodeChunkMetadata{
				"old-chunk": {ID: "old-chunk", Path: "main.go", Hash: "old-hash", Language: "go", StartLine: 1, EndLine: 1},
			},
		}
		require.NoError(t, k.saveCodeIndexManifest(ctx, oldManifest, ""))

		_, err := k.IndexCodebase(ctx, CodeIndexOptions{KBID: "code", Root: root, IncludeUntracked: true})
		require.Error(t, err)
		require.Empty(t, format.deletedIDs)

		manifest, _, err := k.loadCodeIndexManifest(ctx, "code")
		require.NoError(t, err)
		require.Contains(t, manifest.Chunks, "old-chunk")
		require.Equal(t, []string{"old-chunk"}, manifest.Files["main.go"].ChunkIDs)
	})
}

func TestCodeHooks(t *testing.T) {
	t.Run("install status uninstall", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")

		status, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, KBID: "code", Binary: "minnow"})
		require.NoError(t, err)
		for _, hook := range CodeHookNames {
			require.True(t, status.Installed[hook], hook)
		}

		status, err = UninstallCodeIndexHooks(ctx, root)
		require.NoError(t, err)
		for _, hook := range CodeHookNames {
			require.False(t, status.Installed[hook], hook)
		}
	})

	t.Run("refuses existing hook", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		hooksDir := filepath.Join(root, ".git", "hooks")
		writeTestFile(t, filepath.Join(hooksDir, "post-commit"), "#!/bin/sh\necho existing\n")

		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, KBID: "code"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists")

		_, err = InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, KBID: "code", Force: true})
		require.NoError(t, err)
	})

	t.Run("renders index key confirmation and quoted binary", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")

		_, err := InstallCodeIndexHooks(ctx, CodeHookOptions{Root: root, KBID: "code-api", IndexKey: "api", Binary: "/tmp/minnow bin"})
		require.NoError(t, err)
		data, err := os.ReadFile(filepath.Join(root, ".git", "hooks", "post-commit"))
		require.NoError(t, err)
		content := string(data)
		require.Contains(t, content, `"/tmp/minnow bin" index refresh`)
		require.Contains(t, content, `--kb "code-api"`)
		require.Contains(t, content, `--index-key "api"`)
		require.Contains(t, content, `--yes`)
	})
}

type failingCodeIndexStreamFormat struct {
	err        error
	deletedIDs []string
}

func (f *failingCodeIndexStreamFormat) Kind() string { return "failing-code-index" }

func (f *failingCodeIndexStreamFormat) Version() int { return 1 }

func (f *failingCodeIndexStreamFormat) FileExt() string { return ".fail" }

func (f *failingCodeIndexStreamFormat) BuildArtifacts(context.Context, string, string, int64) ([]SnapshotShardMetadata, error) {
	return nil, nil
}

func (f *failingCodeIndexStreamFormat) QueryRag(context.Context, RagQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}

func (f *failingCodeIndexStreamFormat) QueryGraph(context.Context, GraphQueryRequest) ([]ExpandedResult, error) {
	return nil, nil
}

func (f *failingCodeIndexStreamFormat) Ingest(context.Context, IngestUpsertRequest) (IngestResult, error) {
	return IngestResult{}, nil
}

func (f *failingCodeIndexStreamFormat) Delete(_ context.Context, req IngestDeleteRequest) (IngestResult, error) {
	f.deletedIDs = append(f.deletedIDs, req.DocIDs...)
	return IngestResult{MutatedCount: len(req.DocIDs)}, nil
}

func (f *failingCodeIndexStreamFormat) PublishPreparedStream(context.Context, PreparedStreamRequest) (IngestResult, error) {
	return IngestResult{}, f.err
}

func TestCodeRegistry(t *testing.T) {
	t.Run("resolves entry", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")

		registry, err := LoadCodebaseIndexRegistry(root)
		require.NoError(t, err)
		registry.Indexes["default"] = CodebaseIndexRegistryEntry{
			KBID:             "minnow",
			Root:             ".",
			Description:      "Default codebase index for Minnow",
			IncludeUntracked: true,
		}
		require.NoError(t, saveCodebaseIndexRegistry(root, registry))

		selection, err := ResolveCodeIndexSelection(root, "default", "")
		require.NoError(t, err)
		require.Equal(t, "default", selection.IndexKey)
		require.Equal(t, "minnow", selection.KBID)
		require.Equal(t, "Default codebase index for Minnow", selection.Description)
		require.True(t, selection.IncludeUntracked)

		data, err := os.ReadFile(filepath.Join(root, ".minnow", "codebase-indexes.json"))
		require.NoError(t, err)
		var decoded struct {
			Indexes map[string]CodebaseIndexRegistryEntry `json:"codebase_indexes"`
		}
		require.NoError(t, json.Unmarshal(data, &decoded))
		require.Equal(t, "minnow", decoded.Indexes["default"].KBID)
	})

	t.Run("falls back from key", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")

		selection, err := ResolveCodeIndexSelection(root, "backend api", "")
		require.NoError(t, err)
		require.Equal(t, "backend-api", selection.IndexKey)
		require.Equal(t, "code-backend-api", selection.KBID)
	})

	t.Run("preserves subtree", func(t *testing.T) {
		root := t.TempDir()
		runGit(t, root, "init")
		writeTestFile(t, filepath.Join(root, "docs", "guide.md"), "# guide\n")
		writeTestFile(t, filepath.Join(root, "main.go"), "package main\n")
		runGit(t, root, "add", ".")

		registry, err := LoadCodebaseIndexRegistry(root)
		require.NoError(t, err)
		registry.Indexes["docs"] = CodebaseIndexRegistryEntry{KBID: "code-docs", Root: "docs", Description: "Docs index"}
		require.NoError(t, saveCodebaseIndexRegistry(root, registry))

		selection, err := ResolveCodeIndexSelection(root, "docs", "")
		require.NoError(t, err)
		require.Equal(t, "code-docs", selection.KBID)
		wantRoot, err := filepath.EvalSymlinks(filepath.Join(root, "docs"))
		require.NoError(t, err)
		require.Equal(t, wantRoot, selection.Root)

		files, _, err := scanCodebase(context.Background(), selection.Root, normalizeCodeIndexOptions(CodeIndexOptions{IncludeUntracked: true}))
		require.NoError(t, err)
		require.Len(t, files, 1)
		require.Equal(t, "guide.md", files[0].RelPath)
	})
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}
