package duckdb

import (
	"os"
	"path/filepath"
	"strings"
)

const localShardManifestVersionFile = "manifest.version"

func localShardManifestVersionPath(kbDir string) string {
	return filepath.Join(kbDir, localShardManifestVersionFile)
}

func readLocalShardManifestVersion(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func writeLocalShardManifestVersion(path, version string) error {
	return os.WriteFile(path, []byte(strings.TrimSpace(version)), 0o644)
}
