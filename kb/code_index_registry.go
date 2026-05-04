package kb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func ResolveCodeIndexSelection(root, indexKey, kbID string) (CodeIndexOptions, error) {
	requestedRoot, err := resolveRequestedCodeRoot(root)
	if err != nil {
		return CodeIndexOptions{}, err
	}
	registryRoot, err := resolveCodeRoot(requestedRoot)
	if err != nil {
		return CodeIndexOptions{}, err
	}
	opts := normalizeCodeIndexOptions(CodeIndexOptions{Root: requestedRoot, IndexKey: indexKey, KBID: kbID})
	registry, err := loadCodebaseIndexRegistry(registryRoot)
	if err != nil {
		return CodeIndexOptions{}, err
	}
	if entry, ok := registry.Indexes[opts.IndexKey]; ok {
		if strings.TrimSpace(opts.KBID) == "" {
			opts.KBID = entry.KBID
		}
		opts.Root = rootFromRegistryEntry(registryRoot, entry)
		opts.Description = entry.Description
		opts.IncludeUntracked = entry.IncludeUntracked
	}
	if strings.TrimSpace(opts.KBID) == "" {
		opts.KBID = defaultKBIDForIndexKey(opts.IndexKey)
	}
	return opts, nil
}

func LoadCodebaseIndexRegistry(root string) (CodebaseIndexRegistry, error) {
	requestedRoot, err := resolveRequestedCodeRoot(root)
	if err != nil {
		return CodebaseIndexRegistry{}, err
	}
	registryRoot, err := resolveCodeRoot(requestedRoot)
	if err != nil {
		return CodebaseIndexRegistry{}, err
	}
	return loadCodebaseIndexRegistry(registryRoot)
}

func loadCodebaseIndexRegistry(root string) (CodebaseIndexRegistry, error) {
	registry := CodebaseIndexRegistry{SchemaVersion: "minnow.codebase_indexes/v1", Indexes: map[string]CodebaseIndexRegistryEntry{}}
	data, err := os.ReadFile(codebaseIndexRegistryPath(root))
	if err != nil {
		if os.IsNotExist(err) {
			return registry, nil
		}
		return CodebaseIndexRegistry{}, err
	}
	if err := json.Unmarshal(data, &registry); err != nil {
		return CodebaseIndexRegistry{}, err
	}
	if registry.Indexes == nil {
		registry.Indexes = map[string]CodebaseIndexRegistryEntry{}
	}
	return registry, nil
}

func saveCodebaseIndexRegistry(root string, registry CodebaseIndexRegistry) error {
	if registry.SchemaVersion == "" {
		registry.SchemaVersion = "minnow.codebase_indexes/v1"
	}
	if registry.Indexes == nil {
		registry.Indexes = map[string]CodebaseIndexRegistryEntry{}
	}
	path := codebaseIndexRegistryPath(root)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(registry, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func codebaseIndexRegistryPath(root string) string {
	return filepath.Join(root, ".minnow", "codebase-indexes.json")
}

func sanitizeCodeIndexKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return "default"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '.':
			return r
		default:
			return '-'
		}
	}, key)
}

func defaultKBIDForIndexKey(key string) string {
	key = sanitizeCodeIndexKey(key)
	return "code-" + key
}

func defaultCodeIndexDescription(root, key string) string {
	name := filepath.Base(root)
	if key == "default" {
		return "Default codebase index for " + name
	}
	return fmt.Sprintf("Codebase index %q for %s", key, name)
}

func resolveRequestedCodeRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("code index root must be a directory: %s", abs)
	}
	return filepath.Clean(abs), nil
}

func registryRelativeRoot(registryRoot, root string) string {
	rel, err := filepath.Rel(registryRoot, root)
	if err != nil || rel == "." {
		return "."
	}
	return filepath.ToSlash(rel)
}

func rootFromRegistryEntry(registryRoot string, entry CodebaseIndexRegistryEntry) string {
	if entry.Root == "" || entry.Root == "." {
		return registryRoot
	}
	return filepath.Clean(filepath.Join(registryRoot, filepath.FromSlash(entry.Root)))
}

func resolveCodeRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		root = "."
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if out, err := exec.Command("git", "-C", abs, "rev-parse", "--show-toplevel").Output(); err == nil {
		candidate := strings.TrimSpace(string(out))
		if candidate != "" {
			abs = candidate
		}
	}
	info, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("code index root must be a directory: %s", abs)
	}
	return filepath.Clean(abs), nil
}

func codeRepoID(root string) string {
	if out, err := exec.Command("git", "-C", root, "config", "--get", "remote.origin.url").Output(); err == nil {
		if remote := strings.TrimSpace(string(out)); remote != "" {
			sum := sha256.Sum256([]byte(remote))
			return hex.EncodeToString(sum[:8])
		}
	}
	sum := sha256.Sum256([]byte(filepath.Clean(root)))
	return hex.EncodeToString(sum[:8])
}

func codeIndexManifestKey(kbID string) string { return kbID + ".code-index.json" }

func (k *KB) loadCodeIndexManifest(ctx context.Context, kbID string) (codeIndexManifest, string, error) {
	key := codeIndexManifestKey(kbID)
	info, err := k.BlobStore.Head(ctx, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return codeIndexManifest{SchemaVersion: CodeIndexManifestSchema, KBID: kbID, Files: map[string]codeIndexedFile{}, Chunks: map[string]CodeChunkMetadata{}}, "", nil
		}
		return codeIndexManifest{}, "", err
	}
	tmp, err := os.CreateTemp("", "minnow-code-index-*.json")
	if err != nil {
		return codeIndexManifest{}, "", err
	}
	path := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(path)
	if err := k.BlobStore.Download(ctx, key, path); err != nil {
		return codeIndexManifest{}, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return codeIndexManifest{}, "", err
	}
	var manifest codeIndexManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return codeIndexManifest{}, "", err
	}
	if manifest.SchemaVersion != CodeIndexManifestSchema {
		return codeIndexManifest{}, "", fmt.Errorf("unsupported code index manifest schema %q", manifest.SchemaVersion)
	}
	if manifest.Files == nil {
		manifest.Files = map[string]codeIndexedFile{}
	}
	if manifest.Chunks == nil {
		manifest.Chunks = map[string]CodeChunkMetadata{}
	}
	return manifest, info.Version, nil
}

func (k *KB) saveCodeIndexManifest(ctx context.Context, manifest codeIndexManifest, expectedVersion string) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "minnow-code-index-*.json")
	if err != nil {
		return err
	}
	path := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(path)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	defer os.Remove(path)
	_, err = k.BlobStore.UploadIfMatch(ctx, codeIndexManifestKey(manifest.KBID), path, expectedVersion)
	return err
}
