package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const DefaultExtensionDir = "extensions"

// ResolveExtensionDir walks up from the working directory to find a
// DefaultExtensionDir directory. Returns the absolute path if found, or "".
func ResolveExtensionDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		candidate := filepath.Join(dir, DefaultExtensionDir)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func (f *DuckDBArtifactFormat) openConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	memLimit, err := normalizeDuckDBMemoryLimit(f.deps.MemoryLimit)
	if err != nil {
		return nil, err
	}
	extensionDir, err := normalizeDuckDBExtensionDir(f.deps.ExtensionDir, f.deps.OfflineExt)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}

	if err := configureDuckDB(ctx, db, duckDBRuntimeConfig{extensionDir: extensionDir, memLimit: memLimit, offlineExt: f.deps.OfflineExt, threads: f.deps.DuckDBThreads}); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

type duckDBRuntimeConfig struct {
	extensionDir string
	memLimit     string
	offlineExt   bool
	threads      int
}

func configureDuckDB(ctx context.Context, db *sql.DB, cfg duckDBRuntimeConfig) error {
	if cfg.extensionDir != "" {
		extensionDirSQL := quoteSQLString(cfg.extensionDir)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET extension_directory = '%s'`, extensionDirSQL)); err != nil {
			return fmt.Errorf("set extension_directory: %w", err)
		}
	}
	if err := disableDuckDBImplicitExtensionLoading(ctx, db); err != nil {
		return err
	}
	if err := loadDuckDBVSS(ctx, db, cfg.extensionDir, cfg.offlineExt); err != nil {
		return err
	}
	return configureDuckDBRuntime(ctx, db, cfg.memLimit, cfg.threads)
}

func disableDuckDBImplicitExtensionLoading(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `SET autoinstall_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoinstall: %w", err)
	}
	if _, err := db.ExecContext(ctx, `SET autoload_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoload: %w", err)
	}
	return nil
}

func loadDuckDBVSS(ctx context.Context, db *sql.DB, extensionDir string, offlineExt bool) error {
	if _, err := db.ExecContext(ctx, `LOAD vss`); err == nil {
		return nil
	} else if offlineExt {
		return fmt.Errorf("failed to load vss extension in offline mode (check extension_directory %q): %w", extensionDir, err)
	}
	if _, installErr := db.ExecContext(ctx, `INSTALL vss`); installErr != nil {
		return fmt.Errorf("failed to install vss: %w", installErr)
	}
	if _, loadErr := db.ExecContext(ctx, `LOAD vss`); loadErr != nil {
		return fmt.Errorf("failed to load vss after install: %w", loadErr)
	}
	return nil
}

func configureDuckDBRuntime(ctx context.Context, db *sql.DB, memLimit string, threads int) error {
	if _, err := db.ExecContext(ctx, `SET hnsw_enable_experimental_persistence = true`); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET memory_limit = '%s'`, quoteSQLString(memLimit))); err != nil {
		return err
	}
	if threads <= 0 {
		threads = 1
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf(`PRAGMA threads = %d`, threads))
	return err
}

// OpenConfiguredDB is the exported variant for test helpers.
func (f *DuckDBArtifactFormat) OpenConfiguredDB(ctx context.Context, dbPath string) (*sql.DB, error) {
	return f.openConfiguredDB(ctx, dbPath)
}

// LoadVSS loads the vss extension into a raw *sql.DB.
func (f *DuckDBArtifactFormat) LoadVSS(db *sql.DB) error {
	return loadVSS(db)
}

// loadVSS loads the vss extension into a raw *sql.DB for test fixture
// builders that bypass openConfiguredDB.
func loadVSS(db *sql.DB) error {
	extDir := ResolveExtensionDir()
	if extDir != "" {
		if _, err := db.Exec(fmt.Sprintf(`SET extension_directory = '%s'`, extDir)); err != nil {
			return fmt.Errorf("set extension_directory: %w", err)
		}
	}
	if _, err := db.Exec(`SET autoinstall_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoinstall: %w", err)
	}
	if _, err := db.Exec(`SET autoload_known_extensions = false`); err != nil {
		return fmt.Errorf("disable autoload: %w", err)
	}
	if _, err := db.Exec(`LOAD vss`); err != nil {
		return fmt.Errorf("load vss: %w", err)
	}
	return nil
}

var duckDBMemoryLimitPattern = regexp.MustCompile(`(?i)^\s*\d+\s*(b|kb|mb|gb|tb)\s*$`)

func normalizeDuckDBMemoryLimit(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		trimmed = "128MB"
	}
	if !duckDBMemoryLimitPattern.MatchString(trimmed) {
		return "", fmt.Errorf("invalid memory limit %q", raw)
	}
	return trimmed, nil
}

func normalizeDuckDBExtensionDir(raw string, offline bool) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	if strings.ContainsRune(trimmed, '\x00') {
		return "", fmt.Errorf("invalid extension directory %q", raw)
	}

	cleaned := filepath.Clean(trimmed)
	info, err := os.Stat(cleaned)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if offline {
				return "", fmt.Errorf("extension directory %q does not exist in offline mode", cleaned)
			}
			return cleaned, nil
		}
		return "", fmt.Errorf("stat extension directory %q: %w", cleaned, err)
	}

	if !info.IsDir() {
		return "", fmt.Errorf("extension directory %q is not a directory", cleaned)
	}

	return cleaned, nil
}

func quoteSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
