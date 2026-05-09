package codeindex

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHooks(t *testing.T) {
	t.Run("install and uninstall", func(t *testing.T) {
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

	t.Run("command arguments are quoted", func(t *testing.T) {
		ctx := context.Background()
		root := t.TempDir()
		runGit(t, root, "init")
		_, err := InstallCodeIndexHooks(
			ctx,
			CodeHookOptions{Root: root, KBID: "code-api", IndexKey: "api", Binary: "/tmp/minnow bin"},
		)
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
