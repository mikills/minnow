package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	kb "github.com/mikills/kbcore/kb"
)

const DefaultExtensionDir = ".duckdb/extensions"

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

// NewArtifactFormat creates a new DuckDBArtifactFormat from the given deps.
func NewArtifactFormat(deps DuckDBArtifactDeps) (*DuckDBArtifactFormat, error) {
	if err := deps.validate(); err != nil {
		return nil, fmt.Errorf("duckdb artifact format: %v", err)
	}
	return &DuckDBArtifactFormat{deps: deps}, nil
}

type DuckDBArtifactFormat struct {
	deps DuckDBArtifactDeps
	pool shardConnPool
}

type DuckDBArtifactDeps struct {
	BlobStore      kb.BlobStore
	ManifestStore  kb.ManifestStore
	CacheDir       string
	MemoryLimit    string
	ExtensionDir   string
	OfflineExt     bool
	ShardingPolicy kb.ShardingPolicy

	Embed        func(context.Context, string) ([]float32, error)
	GraphBuilder func() *kb.GraphBuilder

	EvictCacheIfNeeded         func(context.Context, string) error
	LockFor                    func(string) *sync.Mutex
	AcquireWriteLease          func(ctx context.Context, kbID string) (kb.WriteLeaseManager, *kb.WriteLease, error)
	EnqueueReplacedShardsForGC func(kbID string, shards []kb.SnapshotShardMetadata, now time.Time)
	Metrics                    kb.ShardMetricsObserver
}

func (d DuckDBArtifactDeps) validate() error {
	if d.BlobStore == nil {
		return fmt.Errorf("blob store is required")
	}
	if d.ManifestStore == nil {
		return fmt.Errorf("manifest store is required")
	}
	if d.Embed == nil {
		return fmt.Errorf("embed function is required")
	}
	if d.GraphBuilder == nil {
		return fmt.Errorf("graph builder function is required")
	}
	if d.EvictCacheIfNeeded == nil {
		return fmt.Errorf("cache eviction function is required")
	}
	if d.LockFor == nil {
		return fmt.Errorf("lock provider function is required")
	}
	if d.Metrics == nil {
		return fmt.Errorf("shard metrics observer is required")
	}
	if kb.NormalizeShardingPolicy(d.ShardingPolicy).CompactionEnabled && d.AcquireWriteLease == nil {
		return fmt.Errorf("AcquireWriteLease is required when compaction is enabled")
	}
	return nil
}

const (
	DuckDBFormatKind    = "duckdb_sharded"
	DuckDBFormatVersion = 1
)

func (f *DuckDBArtifactFormat) validateManifestFormat(manifest *kb.SnapshotShardManifest) error {
	if manifest.FormatKind != DuckDBFormatKind {
		return fmt.Errorf("%w: manifest format_kind %q does not match expected %q", kb.ErrArtifactFormatNotConfigured, manifest.FormatKind, DuckDBFormatKind)
	}
	if manifest.FormatVersion != DuckDBFormatVersion {
		return fmt.Errorf("%w: manifest format_version %d does not match expected %d", kb.ErrArtifactFormatNotConfigured, manifest.FormatVersion, DuckDBFormatVersion)
	}
	return nil
}

// Close drains the shard connection pool.
func (f *DuckDBArtifactFormat) Close() {
	f.pool.CloseAll()
}

// ClosePooledConns closes pooled connections matching the path prefix.
// Satisfies the PooledConnCloser interface used by cache eviction.
func (f *DuckDBArtifactFormat) ClosePooledConns(pathPrefix string) {
	f.pool.CloseByPrefix(pathPrefix)
}

func (f *DuckDBArtifactFormat) Kind() string {
	return DuckDBFormatKind
}

func (f *DuckDBArtifactFormat) Version() int {
	return DuckDBFormatVersion
}

func (f *DuckDBArtifactFormat) FileExt() string {
	return ".duckdb"
}

func (f *DuckDBArtifactFormat) lockFor(kbID string) *sync.Mutex {
	return f.deps.LockFor(kbID)
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

	if extensionDir != "" {
		extensionDirSQL := quoteSQLString(extensionDir)
		if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET extension_directory = '%s'`, extensionDirSQL)); err != nil {
			db.Close()
			return nil, fmt.Errorf("set extension_directory: %w", err)
		}
	}

	if f.deps.OfflineExt {
		if _, err := db.ExecContext(ctx, `SET autoinstall_known_extensions = false`); err != nil {
			db.Close()
			return nil, fmt.Errorf("disable autoinstall: %w", err)
		}
	}

	if _, err := db.ExecContext(ctx, `LOAD vss`); err != nil {
		if f.deps.OfflineExt {
			db.Close()
			return nil, fmt.Errorf("failed to load vss extension in offline mode (check extension_directory %q): %w", extensionDir, err)
		}
		if _, installErr := db.ExecContext(ctx, `INSTALL vss`); installErr != nil {
			db.Close()
			return nil, fmt.Errorf("failed to install vss: %w", installErr)
		}
		if _, loadErr := db.ExecContext(ctx, `LOAD vss`); loadErr != nil {
			db.Close()
			return nil, fmt.Errorf("failed to load vss after install: %w", loadErr)
		}
	}

	if _, err := db.ExecContext(ctx, `SET hnsw_enable_experimental_persistence = true`); err != nil {
		db.Close()
		return nil, err
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET memory_limit = '%s'`, quoteSQLString(memLimit))); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `PRAGMA threads = 1`); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
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

func (f *DuckDBArtifactFormat) BuildArtifacts(ctx context.Context, kbID, srcPath string, targetBytes int64) ([]kb.SnapshotShardMetadata, error) {
	if targetBytes <= 0 {
		targetBytes = kb.DefaultSnapshotShardSize
	}
	dbHash, err := kb.FileContentSHA256(srcPath)
	if err != nil {
		return nil, err
	}
	prefix := fmt.Sprintf("%s.duckdb.shards/%s", kbID, dbHash[:16])
	parts, _, err := f.uploadQueryableSnapshotShards(ctx, prefix, srcPath, targetBytes)
	if err != nil {
		return nil, err
	}
	return parts, nil
}

func (f *DuckDBArtifactFormat) QueryRag(ctx context.Context, req kb.RagQueryRequest) ([]kb.ExpandedResult, error) {
	if err := kb.ValidateRagQueryRequest(req); err != nil {
		return nil, err
	}

	results, err := f.searchTopK(ctx, req.KBID, req.QueryVec, req.Options.TopK)
	if err != nil {
		return nil, err
	}
	expanded := kb.ExpandedFromVector(results)
	if req.Options.MaxDistance == nil {
		return expanded, nil
	}
	maxDistance := *req.Options.MaxDistance
	filtered := make([]kb.ExpandedResult, 0, len(expanded))
	for _, r := range expanded {
		if r.Distance <= maxDistance {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func (f *DuckDBArtifactFormat) QueryGraph(ctx context.Context, req kb.GraphQueryRequest) ([]kb.ExpandedResult, error) {
	if err := kb.ValidateGraphQueryRequest(req); err != nil {
		return nil, err
	}
	if err := f.ensureGraphModeAvailable(ctx, req.KBID); err != nil {
		return nil, err
	}

	options := kb.NormalizeExpansionOptions(req.Options.TopK, req.Options.Expansion)
	options.OfflineExt = f.deps.OfflineExt
	selection, err := f.resolveVectorQuerySelection(ctx, req.KBID, req.QueryVec)
	if err != nil {
		return nil, fmt.Errorf("select vector query path: %w", err)
	}
	parallelism := selection.Plan.Parallelism
	if parallelism <= 0 {
		parallelism = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([][]kb.ExpandedResult, len(selection.Plan.Shards))
	errCh := make(chan error, 1)
	sem := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	graphShardCount := 0
	for i := range selection.Plan.Shards {
		idx := i
		shard := selection.Plan.Shards[i]
		if !shard.GraphAvailable {
			continue
		}
		graphShardCount++
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			localPath, _, err := f.ensureLocalShardFile(ctx, req.KBID, shard)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			conn, err := f.pool.GetOrOpen(ctx, localPath, f.openConfiguredDB)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			defer conn.mu.Unlock()

			shardResults, err := searchExpandedWithDB(ctx, conn.db, req.QueryVec, req.Options.TopK, options)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
				return
			}
			results[idx] = shardResults
		}()
	}
	if graphShardCount == 0 {
		return nil, kb.ErrGraphQueryUnavailable
	}

	wg.Wait()
	select {
	case err := <-errCh:
		return nil, err
	default:
	}

	merged := mergeExpandedShardResults(results, req.Options.TopK)
	if req.Options.MaxDistance == nil {
		return merged, nil
	}
	maxDistance := *req.Options.MaxDistance
	filtered := make([]kb.ExpandedResult, 0, len(merged))
	for _, r := range merged {
		if r.Distance <= maxDistance {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func (f *DuckDBArtifactFormat) Ingest(ctx context.Context, req kb.IngestUpsertRequest) (kb.IngestResult, error) {
	if req.KBID == "" {
		return kb.IngestResult{}, fmt.Errorf("kbID cannot be empty")
	}
	if len(req.Docs) == 0 {
		return kb.IngestResult{}, nil
	}

	preparedDocs, err := f.prepareDocsForUpsert(ctx, req.Docs)
	if err != nil {
		return kb.IngestResult{}, err
	}

	graphBuilder := f.deps.GraphBuilder()
	if req.Options.GraphEnabled != nil {
		if *req.Options.GraphEnabled {
			if graphBuilder == nil {
				return kb.IngestResult{}, kb.ErrGraphUnavailable
			}
		} else {
			graphBuilder = nil
		}
	}

	lock := f.lockFor(req.KBID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, req.KBID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	seedVec := preparedDocs[0].Embedding
	if len(seedVec) == 0 {
		return kb.IngestResult{}, fmt.Errorf("embed doc %q for shard bootstrap: empty embedding", req.Docs[0].ID)
	}
	if err := f.ensureMutableShardDBLocked(ctx, req.KBID, kbDir, dbPath, len(seedVec), true); err != nil {
		return kb.IngestResult{}, err
	}

	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return kb.IngestResult{}, err
	}
	defer db.Close()

	if err := f.applyUpsert(ctx, db, preparedDocs, graphBuilder); err != nil {
		return kb.IngestResult{}, err
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if err := f.postMutationCommit(ctx, db, req.KBID, req.Upload, policy.TargetShardBytes); err != nil {
		return kb.IngestResult{}, err
	}
	if req.Upload {
		if err := f.cleanupLegacySnapshotObjectsBestEffort(ctx, req.KBID); err != nil {
			return kb.IngestResult{}, err
		}
	}

	return kb.IngestResult{MutatedCount: len(req.Docs)}, nil
}

func (f *DuckDBArtifactFormat) Delete(ctx context.Context, req kb.IngestDeleteRequest) (kb.IngestResult, error) {
	if req.KBID == "" {
		return kb.IngestResult{}, fmt.Errorf("kbID cannot be empty")
	}
	if len(req.DocIDs) == 0 {
		return kb.IngestResult{}, nil
	}

	cleanIDs := make([]string, 0, len(req.DocIDs))
	for _, id := range req.DocIDs {
		trimmed := strings.TrimSpace(id)
		if trimmed == "" {
			return kb.IngestResult{}, fmt.Errorf("doc id cannot be empty")
		}
		cleanIDs = append(cleanIDs, trimmed)
	}

	lock := f.lockFor(req.KBID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, req.KBID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	if err := f.ensureMutableShardDBLocked(ctx, req.KBID, kbDir, dbPath, 0, false); err != nil {
		return kb.IngestResult{}, err
	}

	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return kb.IngestResult{}, err
	}
	defer db.Close()

	if err := f.applyDelete(ctx, db, cleanIDs, req.Options); err != nil {
		return kb.IngestResult{}, err
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if err := f.postMutationCommit(ctx, db, req.KBID, req.Upload, policy.TargetShardBytes); err != nil {
		return kb.IngestResult{}, err
	}

	return kb.IngestResult{MutatedCount: len(cleanIDs)}, nil
}

func (f *DuckDBArtifactFormat) ensureMutableShardDBLocked(
	ctx context.Context,
	kbID string,
	kbDir string,
	dbPath string,
	embeddingDim int,
	allowBootstrap bool,
) error {
	if strings.TrimSpace(kbID) == "" {
		return fmt.Errorf("kbID cannot be empty")
	}

	manifestVersion, err := f.deps.ManifestStore.HeadVersion(ctx, kbID)
	if err != nil {
		return err
	}
	if manifestVersion == "" {
		if !allowBootstrap {
			return kb.ErrKBUninitialized
		}
		if embeddingDim <= 0 {
			return fmt.Errorf("embedding dimension must be > 0")
		}
		return f.createEmptyMutableShardDBLocked(ctx, kbDir, dbPath, embeddingDim)
	}

	localVersionPath := localShardManifestVersionPath(kbDir)
	localVersion, readErr := readLocalShardManifestVersion(localVersionPath)
	hasLocalVersion := readErr == nil
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	if hasLocalVersion && localVersion == manifestVersion {
		if _, statErr := os.Stat(dbPath); statErr == nil {
			return nil
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
	}

	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		return err
	}
	if _, err := f.downloadSnapshotFromShards(ctx, kbID, dbPath); err != nil {
		return err
	}
	if err := writeLocalShardManifestVersion(localVersionPath, manifestVersion); err != nil {
		return err
	}

	return nil
}

func (f *DuckDBArtifactFormat) createEmptyMutableShardDBLocked(ctx context.Context, kbDir, dbPath string, embeddingDim int) error {
	if embeddingDim <= 0 {
		return fmt.Errorf("embedding dimension must be > 0")
	}
	if err := os.MkdirAll(kbDir, 0o755); err != nil {
		return err
	}

	db, err := f.openConfiguredDB(ctx, dbPath)
	if err != nil {
		return err
	}

	createDocsSQL := `
		CREATE TABLE IF NOT EXISTS docs (
			id TEXT,
			content TEXT,
			embedding FLOAT[` + strconv.Itoa(embeddingDim) + `]
		)
	`
	if _, err := db.ExecContext(ctx, createDocsSQL); err != nil {
		_ = db.Close()
		return fmt.Errorf("create docs table for shard bootstrap: %w", err)
	}
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	if err := EnsureGraphTables(ctx, db); err != nil {
		_ = db.Close()
		return err
	}

	if err := CheckpointAndCloseDB(ctx, db, "close db after shard bootstrap"); err != nil {
		return err
	}
	return nil
}

func (f *DuckDBArtifactFormat) postMutationCommit(ctx context.Context, db *sql.DB, kbID string, upload bool, targetShardBytes int64) error {
	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")

	dbClosed := false
	defer func() {
		if !dbClosed {
			_ = db.Close()
		}
	}()

	if err := CheckpointDB(ctx, db); err != nil {
		return err
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close db after mutation: %w", err)
	}
	dbClosed = true

	if upload {
		expectedManifestVersion, err := f.deps.ManifestStore.HeadVersion(ctx, kbID)
		if err != nil {
			return err
		}

		artifacts, err := f.BuildArtifacts(ctx, kbID, dbPath, targetShardBytes)
		if err != nil {
			return err
		}

		totalSize := int64(0)
		for _, a := range artifacts {
			totalSize += a.SizeBytes
		}
		manifest := kb.SnapshotShardManifest{
			SchemaVersion:  1,
			Layout:         kb.ShardManifestLayoutDuckDBs,
			FormatKind:     DuckDBFormatKind,
			FormatVersion:  DuckDBFormatVersion,
			KBID:           kbID,
			CreatedAt:      time.Now().UTC(),
			TotalSizeBytes: totalSize,
			Shards:         artifacts,
		}

		newVersion, err := f.deps.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expectedManifestVersion)
		if err != nil {
			if errors.Is(err, kb.ErrBlobVersionMismatch) {
				f.deps.Metrics.RecordShardCount(kbID, len(artifacts))
			}
			return err
		}
		f.deps.Metrics.RecordShardCount(kbID, len(artifacts))
		if err := writeLocalShardManifestVersion(localShardManifestVersionPath(kbDir), newVersion); err != nil {
			return err
		}
	}

	if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
		return err
	}

	return nil
}

func (f *DuckDBArtifactFormat) cleanupLegacySnapshotObjectsBestEffort(ctx context.Context, kbID string) error {
	legacyKeys := []string{
		kbID + ".duckdb",
		kbID + ".snapshot.json",
	}
	for _, key := range legacyKeys {
		err := f.deps.BlobStore.Delete(ctx, key)
		if err == nil || errors.Is(err, kb.ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			continue
		}
		slog.Default().WarnContext(ctx, "failed to delete legacy snapshot object", "kb_id", kbID, "key", key, "error", err)
	}
	return nil
}

// PrepareAndOpenDB prepares the working state and opens the DB for direct access.
func (f *DuckDBArtifactFormat) PrepareAndOpenDB(ctx context.Context, kbID string) (*sql.DB, error) {
	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")
	if err := f.ensureMutableShardDBLocked(ctx, kbID, kbDir, dbPath, 0, false); err != nil {
		return nil, err
	}
	if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
		return nil, err
	}
	return f.openConfiguredDB(ctx, dbPath)
}

func (f *DuckDBArtifactFormat) uploadQueryableSnapshotShards(ctx context.Context, prefix, localDBPath string, partSize int64) ([]kb.SnapshotShardMetadata, int64, error) {
	// Open source DB only for metadata queries, then close before building
	// shards. DuckDB v1.5+ does not allow a file to be open by multiple
	// database handles simultaneously, and buildShardDBFromSourceRange
	// ATTACHes localDBPath from a separate shard DB handle.
	hasTombstones, activeRows, err := func() (bool, int64, error) {
		sourceDB, err := f.openConfiguredDB(ctx, localDBPath)
		if err != nil {
			return false, 0, err
		}
		defer sourceDB.Close()

		ht, err := tableExists(ctx, sourceDB, "doc_tombstones")
		if err != nil {
			return false, 0, err
		}
		ar, err := countActiveDocs(ctx, sourceDB, ht)
		if err != nil {
			return false, 0, err
		}
		return ht, ar, nil
	}()
	if err != nil {
		return nil, 0, err
	}
	if activeRows <= 0 {
		return nil, 0, fmt.Errorf("cannot shard snapshot with zero active docs")
	}

	sourceInfo, err := os.Stat(localDBPath)
	if err != nil {
		return nil, 0, err
	}
	bytesPerRow := sourceInfo.Size() / activeRows
	if bytesPerRow <= 0 {
		bytesPerRow = 1
	}
	rowsPerShard := int(partSize / bytesPerRow)
	if rowsPerShard <= 0 {
		rowsPerShard = 1
	}

	tmpDir, err := os.MkdirTemp("", "kbcore-queryable-shards-*")
	if err != nil {
		return nil, 0, err
	}
	defer os.RemoveAll(tmpDir)

	estimatedShards := int((activeRows + int64(rowsPerShard) - 1) / int64(rowsPerShard))
	parts := make([]kb.SnapshotShardMetadata, 0, estimatedShards)
	totalSize := int64(0)
	processedRows := int64(0)
	lastDocID := ""
	for shardIndex := 0; processedRows < activeRows; shardIndex++ {
		shardPath := filepath.Join(tmpDir, fmt.Sprintf("shard-%05d.duckdb", shardIndex))
		vectorRows, graphAvailable, nextDocID, centroid, err := f.buildShardDBFromSourceRange(ctx, localDBPath, hasTombstones, shardPath, rowsPerShard, lastDocID)
		if err != nil {
			return nil, 0, err
		}
		if vectorRows <= 0 {
			return nil, 0, fmt.Errorf("failed to advance shard cursor while building snapshot shards")
		}
		lastDocID = nextDocID
		processedRows += vectorRows

		sha, err := kb.FileContentSHA256(shardPath)
		if err != nil {
			return nil, 0, err
		}
		info, err := os.Stat(shardPath)
		if err != nil {
			return nil, 0, err
		}
		partKey := fmt.Sprintf("%s/shard-%05d.duckdb", prefix, shardIndex)
		uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, partKey, shardPath, "")
		if err != nil {
			return nil, 0, err
		}

		now := time.Now().UTC()
		parts = append(parts, kb.SnapshotShardMetadata{
			ShardID:        fmt.Sprintf("shard-%05d", shardIndex),
			Key:            partKey,
			Version:        uploadInfo.Version,
			SizeBytes:      info.Size(),
			VectorRows:     vectorRows,
			CreatedAt:      now,
			SealedAt:       now,
			TombstoneRatio: 0,
			GraphAvailable: graphAvailable,
			Centroid:       centroid,
			SHA256:         sha,
		})
		totalSize += info.Size()
	}

	if len(parts) == 0 {
		return nil, 0, fmt.Errorf("failed to build shard files")
	}
	return parts, totalSize, nil
}

func (f *DuckDBArtifactFormat) buildShardDBFromSourceRange(ctx context.Context, sourceDBPath string, hasTombstones bool, shardPath string, limit int, lastDocID string) (int64, bool, string, []float32, error) {
	if limit <= 0 {
		return 0, false, "", nil, fmt.Errorf("limit must be > 0")
	}

	shardDB, err := f.openConfiguredDB(ctx, shardPath)
	if err != nil {
		return 0, false, "", nil, err
	}
	defer shardDB.Close()

	if _, err := shardDB.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS src (READ_ONLY)", quoteSQLString(sourceDBPath))); err != nil {
		return 0, false, "", nil, err
	}
	defer func() {
		_, _ = shardDB.ExecContext(context.Background(), "DETACH src")
	}()

	whereClauses := make([]string, 0, 2)
	if hasTombstones {
		whereClauses = append(whereClauses, "NOT EXISTS (SELECT 1 FROM src.doc_tombstones t WHERE t.doc_id = d.id)")
	}
	if lastDocID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("d.id > '%s'", quoteSQLString(lastDocID)))
	}
	where := ""
	if len(whereClauses) > 0 {
		where = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	createSQL := fmt.Sprintf(`
		CREATE TABLE docs AS
		SELECT d.id, d.content, d.embedding
		FROM src.docs d
		%s
		ORDER BY d.id
		LIMIT %d
	`, where, limit)
	if _, err := shardDB.ExecContext(ctx, createSQL); err != nil {
		return 0, false, "", nil, err
	}

	if _, err := shardDB.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return 0, false, "", nil, err
	}

	if err := EnsureGraphTables(ctx, shardDB); err != nil {
		return 0, false, "", nil, err
	}

	if ok, err := sourceGraphTablesReady(ctx, shardDB); err != nil {
		return 0, false, "", nil, err
	} else if ok {
		if _, err := shardDB.ExecContext(ctx, `
			INSERT INTO doc_entities (doc_id, entity_id, weight, chunk_id)
			SELECT de.doc_id, de.entity_id, de.weight, de.chunk_id
			FROM src.doc_entities de
			JOIN docs d ON d.id = de.doc_id
		`); err != nil {
			return 0, false, "", nil, err
		}
		if _, err := shardDB.ExecContext(ctx, `
			INSERT OR IGNORE INTO entities (id, name)
			SELECT DISTINCT e.id, e.name
			FROM src.entities e
			JOIN doc_entities de ON de.entity_id = e.id
		`); err != nil {
			return 0, false, "", nil, err
		}
		if _, err := shardDB.ExecContext(ctx, `
			INSERT INTO edges (src, dst, weight, rel_type, chunk_id)
			SELECT DISTINCT e.src, e.dst, e.weight, e.rel_type, e.chunk_id
			FROM src.edges e
			JOIN doc_entities de ON de.chunk_id = e.chunk_id
		`); err != nil {
			return 0, false, "", nil, err
		}
	}

	var rowCount int64
	if err := shardDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&rowCount); err != nil {
		return 0, false, "", nil, err
	}
	if rowCount <= 0 {
		if err := CheckpointAndCloseDB(ctx, shardDB, "close empty shard db"); err != nil {
			return 0, false, "", nil, err
		}
		return 0, false, lastDocID, nil, nil
	}

	var maxDocID string
	if err := shardDB.QueryRowContext(ctx, `SELECT MAX(id) FROM docs`).Scan(&maxDocID); err != nil {
		return 0, false, "", nil, err
	}
	centroid, err := computeShardCentroid(ctx, shardDB)
	if err != nil {
		return 0, false, "", nil, err
	}
	graphAvailable := hasGraphQueryData(ctx, shardDB)
	if err := CheckpointAndCloseDB(ctx, shardDB, "close shard db"); err != nil {
		return 0, false, "", nil, err
	}
	return rowCount, graphAvailable, maxDocID, centroid, nil
}

func (f *DuckDBArtifactFormat) downloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*kb.SnapshotShardManifest, error) {
	if kbID == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}
	if dest == "" {
		return nil, fmt.Errorf("dest cannot be empty")
	}

	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		return nil, err
	}
	manifest := doc.Manifest
	if err := f.validateManifestFormat(&manifest); err != nil {
		return nil, err
	}
	f.deps.Metrics.RecordShardCount(kbID, len(manifest.Shards))
	if len(manifest.Shards) == 0 {
		return nil, fmt.Errorf("manifest has no shards")
	}

	tmpDir, err := os.MkdirTemp("", "kbcore-shard-download-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	tmpFile, err := os.CreateTemp(filepath.Dir(dest), "kbcore-reconstruct-*.duckdb")
	if err != nil {
		return nil, err
	}
	tmpDest := tmpFile.Name()
	tmpFile.Close()
	_ = os.Remove(tmpDest)
	defer os.Remove(tmpDest)

	db, err := f.openConfiguredDB(ctx, tmpDest)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	for i, shard := range manifest.Shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("part-%05d.duckdb", i))
		if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
			return nil, err
		}
		info, err := os.Stat(partPath)
		if err != nil {
			return nil, err
		}
		if shard.SizeBytes > 0 && info.Size() != shard.SizeBytes {
			return nil, fmt.Errorf("shard %s size mismatch", shard.ShardID)
		}
		if shard.SHA256 != "" {
			sha, err := kb.FileContentSHA256(partPath)
			if err != nil {
				return nil, err
			}
			if sha != shard.SHA256 {
				return nil, fmt.Errorf("shard %s checksum mismatch", shard.ShardID)
			}
		}

		alias := fmt.Sprintf("s%d", i)
		if err := mergeShardIntoDB(ctx, db, alias, partPath, i == 0); err != nil {
			return nil, err
		}
	}
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return nil, err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return nil, err
	}
	if err := CheckpointAndCloseDB(ctx, db, "close reconstructed shard snapshot"); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpDest, dest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

func (f *DuckDBArtifactFormat) buildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []kb.SnapshotShardMetadata) (kb.SnapshotShardMetadata, error) {
	tmpDir, err := os.MkdirTemp("", "kbcore-compact-*")
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}
	defer os.RemoveAll(tmpDir)

	combinedPath := filepath.Join(tmpDir, "replacement.duckdb")
	db, err := f.openConfiguredDB(ctx, combinedPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}
	defer db.Close()

	vectorRows := int64(0)
	for i, shard := range shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("in-%05d.duckdb", i))
		if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
			return kb.SnapshotShardMetadata{}, err
		}

		alias := fmt.Sprintf("s%d", i)
		if err := mergeShardIntoDB(ctx, db, alias, partPath, i == 0); err != nil {
			return kb.SnapshotShardMetadata{}, err
		}

		vectorRows += shard.VectorRows
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM docs`).Scan(&vectorRows); err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	centroid, err := computeShardCentroid(ctx, db)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	graphAvailable := hasGraphQueryData(ctx, db)
	if err := CheckpointAndCloseDB(ctx, db, "close compacted shard db"); err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	sha, err := kb.FileContentSHA256(combinedPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	info, err := os.Stat(combinedPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	now := time.Now().UTC()
	replacementID := fmt.Sprintf("compact-%d", now.UnixNano())
	replacementKey := fmt.Sprintf("%s.duckdb.compacted/%s/part-00000", kbID, replacementID)
	uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, replacementKey, combinedPath, "")
	if err != nil {
		return kb.SnapshotShardMetadata{}, err
	}

	return kb.SnapshotShardMetadata{
		ShardID:        replacementID,
		Key:            replacementKey,
		Version:        uploadInfo.Version,
		SizeBytes:      info.Size(),
		VectorRows:     vectorRows,
		CreatedAt:      now,
		SealedAt:       now,
		TombstoneRatio: 0,
		GraphAvailable: graphAvailable,
		Centroid:       centroid,
		SHA256:         sha,
	}, nil
}

func (f *DuckDBArtifactFormat) CompactIfNeeded(ctx context.Context, kbID string) (*kb.CompactionPublishResult, error) {
	if strings.TrimSpace(kbID) == "" {
		return nil, fmt.Errorf("kbID cannot be empty")
	}

	lock := f.lockFor(kbID)
	lock.Lock()
	defer lock.Unlock()

	// Acquire cluster-wide write lease to prevent concurrent compactors
	// from duplicating expensive shard builds.
	if f.deps.AcquireWriteLease != nil {
		leaseManager, lease, err := f.deps.AcquireWriteLease(ctx, kbID)
		if err != nil {
			return nil, err
		}
		defer func() { _ = leaseManager.Release(context.Background(), lease) }()
	}

	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return nil, kb.ErrKBUninitialized
		}
		return nil, err
	}

	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return nil, err
	}

	candidates, _ := selectCompactionCandidatesWithReason(f.deps.ShardingPolicy, manifest)
	if len(candidates) < 2 {
		return &kb.CompactionPublishResult{
			Performed:          false,
			ManifestVersionOld: doc.Version,
		}, nil
	}

	replacement, err := f.buildAndUploadCompactionReplacement(ctx, kbID, candidates)
	if err != nil {
		return nil, err
	}

	nextManifest := buildCompactedManifest(kbID, manifest, candidates, replacement)
	newVersion, err := f.deps.ManifestStore.UpsertIfMatch(ctx, kbID, nextManifest, doc.Version)
	if err != nil {
		return nil, err
	}

	if f.deps.EnqueueReplacedShardsForGC != nil {
		f.deps.EnqueueReplacedShardsForGC(kbID, candidates, time.Now().UTC())
	}

	f.deps.Metrics.RecordShardCount(kbID, len(nextManifest.Shards))

	return &kb.CompactionPublishResult{
		Performed:          true,
		ReplacedShards:     append([]kb.SnapshotShardMetadata(nil), candidates...),
		ReplacementShards:  []kb.SnapshotShardMetadata{replacement},
		ManifestVersionOld: doc.Version,
		ManifestVersionNew: newVersion,
	}, nil
}

func computeShardCentroid(ctx context.Context, db *sql.DB) ([]float32, error) {
	rows, err := db.QueryContext(ctx, `SELECT CAST(embedding AS VARCHAR) FROM docs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sums []float64
	count := 0
	for rows.Next() {
		var vecStr string
		if err := rows.Scan(&vecStr); err != nil {
			return nil, err
		}
		vec, err := parseDuckDBVectorString(vecStr)
		if err != nil {
			return nil, err
		}
		if len(vec) == 0 {
			continue
		}
		if sums == nil {
			sums = make([]float64, len(vec))
		}
		if len(vec) != len(sums) {
			return nil, fmt.Errorf("inconsistent embedding dimensions while computing centroid")
		}
		for i := range vec {
			sums[i] += float64(vec[i])
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 || len(sums) == 0 {
		return nil, nil
	}
	centroid := make([]float32, len(sums))
	for i := range sums {
		centroid[i] = float32(sums[i] / float64(count))
	}
	return centroid, nil
}

func parseDuckDBVectorString(raw string) ([]float32, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	trimmed = strings.TrimPrefix(trimmed, "[")
	trimmed = strings.TrimSuffix(trimmed, "]")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	vec := make([]float32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseFloat(p, 32)
		if err != nil {
			return nil, fmt.Errorf("parse vector value %q: %w", p, err)
		}
		vec = append(vec, float32(v))
	}
	return vec, nil
}

// BuildAndUploadCompactionReplacement merges the given shards into one replacement shard and uploads it.
func (f *DuckDBArtifactFormat) BuildAndUploadCompactionReplacement(ctx context.Context, kbID string, shards []kb.SnapshotShardMetadata) (kb.SnapshotShardMetadata, error) {
	return f.buildAndUploadCompactionReplacement(ctx, kbID, shards)
}

// DownloadSnapshotFromShards downloads and reconstructs a sharded snapshot into a single DB file.
func (f *DuckDBArtifactFormat) DownloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*kb.SnapshotShardManifest, error) {
	return f.downloadSnapshotFromShards(ctx, kbID, dest)
}

func (f *DuckDBArtifactFormat) ensureGraphModeAvailable(ctx context.Context, kbID string) error {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		if errors.Is(err, kb.ErrManifestNotFound) {
			return kb.ErrKBUninitialized
		}
		return fmt.Errorf("download shard manifest: %w", err)
	}
	manifest := &doc.Manifest
	if err := f.validateManifestFormat(manifest); err != nil {
		return err
	}
	if len(manifest.Shards) == 0 {
		return kb.ErrGraphQueryUnavailable
	}
	hasGraphShard := false
	for _, shard := range manifest.Shards {
		if shard.GraphAvailable {
			hasGraphShard = true
			break
		}
	}
	if !hasGraphShard {
		return kb.ErrGraphQueryUnavailable
	}

	return nil
}

type preparedUpsertDoc struct {
	Doc       kb.Document
	Embedding []float32
}

func (f *DuckDBArtifactFormat) prepareDocsForUpsert(ctx context.Context, docs []kb.Document) ([]preparedUpsertDoc, error) {
	prepared := make([]preparedUpsertDoc, 0, len(docs))
	for _, doc := range docs {
		if strings.TrimSpace(doc.ID) == "" {
			return nil, fmt.Errorf("doc id cannot be empty")
		}
		if strings.TrimSpace(doc.Text) == "" {
			return nil, fmt.Errorf("doc %q text cannot be empty", doc.ID)
		}

		vec, err := f.deps.Embed(ctx, doc.Text)
		if err != nil {
			return nil, fmt.Errorf("embed doc %q: %w", doc.ID, err)
		}
		if len(vec) == 0 {
			return nil, fmt.Errorf("embed doc %q: empty embedding", doc.ID)
		}

		prepared = append(prepared, preparedUpsertDoc{Doc: doc, Embedding: vec})
	}
	return prepared, nil
}

func (f *DuckDBArtifactFormat) applyUpsert(ctx context.Context, db *sql.DB, docs []preparedUpsertDoc, graphBuilder *kb.GraphBuilder) error {

	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	var graphResult *kb.GraphBuildResult
	var err error
	if graphBuilder != nil {
		graphDocs := make([]kb.Document, 0, len(docs))
		for _, prepared := range docs {
			graphDocs = append(graphDocs, prepared.Doc)
		}
		graphResult, err = graphBuilder.Build(ctx, graphDocs)
		if err != nil {
			return fmt.Errorf("build graph for upsert docs: %w", err)
		}
		if err := EnsureGraphTables(ctx, db); err != nil {
			return err
		}
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert tx: %w", err)
	}

	stmtUndelete, err := tx.PrepareContext(ctx, `DELETE FROM doc_tombstones WHERE doc_id = ?`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare doc_tombstones delete: %w", err)
	}
	defer stmtUndelete.Close()

	for _, prepared := range docs {
		doc := prepared.Doc
		vec := prepared.Embedding
		vecStr := FormatVectorForSQL(vec)
		if _, err := tx.ExecContext(ctx, `DELETE FROM docs WHERE id = ?`, doc.ID); err != nil {
			tx.Rollback()
			return fmt.Errorf("delete existing doc %q before upsert: %w", doc.ID, err)
		}
		upsertSQL := fmt.Sprintf(`INSERT INTO docs (id, content, embedding) VALUES (?, ?, %s::FLOAT[%d])`, vecStr, len(vec))
		if _, err := tx.ExecContext(ctx, upsertSQL, doc.ID, doc.Text); err != nil {
			tx.Rollback()
			return fmt.Errorf("upsert doc %q: %w", doc.ID, err)
		}
		if _, err := stmtUndelete.ExecContext(ctx, doc.ID); err != nil {
			tx.Rollback()
			return fmt.Errorf("clear tombstone for doc %q: %w", doc.ID, err)
		}
	}

	if graphResult != nil {
		docIDs := make([]string, 0, len(docs))
		for _, prepared := range docs {
			docIDs = append(docIDs, prepared.Doc.ID)
		}
		if err := pruneGraphForDocsTx(ctx, tx, docIDs, true); err != nil {
			tx.Rollback()
			return err
		}
		if err := InsertGraphBuildResultTx(ctx, tx, graphResult); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit upsert tx: %w", err)
	}

	return nil
}

func (f *DuckDBArtifactFormat) applyDelete(ctx context.Context, db *sql.DB, cleanIDs []string, opts kb.DeleteDocsOptions) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete docs tx: %w", err)
	}

	placeholders := kb.BuildInClausePlaceholders(len(cleanIDs))
	args := make([]any, 0, len(cleanIDs))
	for _, id := range cleanIDs {
		args = append(args, id)
	}

	if opts.HardDelete {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM docs WHERE id IN (%s)`, placeholders), args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("hard delete docs: %w", err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM doc_tombstones WHERE doc_id IN (%s)`, placeholders), args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("remove tombstones: %w", err)
		}
	} else {
		stmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO doc_tombstones (doc_id, deleted_at) VALUES (?, NOW())`)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("prepare tombstone upsert: %w", err)
		}
		for _, id := range cleanIDs {
			if _, err := stmt.ExecContext(ctx, id); err != nil {
				stmt.Close()
				tx.Rollback()
				return fmt.Errorf("soft delete doc %q: %w", id, err)
			}
		}
		if err := stmt.Close(); err != nil {
			tx.Rollback()
			return fmt.Errorf("close tombstone stmt: %w", err)
		}
	}

	if err := pruneGraphForDocsTx(ctx, tx, cleanIDs, opts.HardDelete && opts.CleanupGraph); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete docs tx: %w", err)
	}

	return nil
}

// mergeShardIntoDB ATTACHes a shard file under alias, copies docs and graph
// tables into db, then DETACHes via defer. isFirst signals that the target
// tables must be created before inserting.
func mergeShardIntoDB(ctx context.Context, db *sql.DB, alias, partPath string, isFirst bool) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", quoteSQLString(partPath), alias)); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.Background(), fmt.Sprintf("DETACH %s", alias)) }()

	if isFirst {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE docs AS SELECT * FROM %s.docs WHERE 1=0", alias)); err != nil {
			return err
		}
		if err := EnsureGraphTables(ctx, db); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO docs SELECT * FROM %s.docs", alias)); err != nil {
		return err
	}
	if ok, err := attachedTableExists(ctx, db, alias, "doc_entities"); err != nil {
		return err
	} else if ok {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO doc_entities SELECT * FROM %s.doc_entities", alias)); err != nil {
			return err
		}
	}
	if ok, err := attachedTableExists(ctx, db, alias, "entities"); err != nil {
		return err
	} else if ok {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT OR IGNORE INTO entities SELECT * FROM %s.entities", alias)); err != nil {
			return err
		}
	}
	if ok, err := attachedTableExists(ctx, db, alias, "edges"); err != nil {
		return err
	} else if ok {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO edges SELECT * FROM %s.edges", alias)); err != nil {
			return err
		}
	}
	return nil
}

func countActiveDocs(ctx context.Context, db *sql.DB, hasTombstones bool) (int64, error) {
	query := `SELECT COUNT(*) FROM docs d`
	if hasTombstones {
		query += ` WHERE NOT EXISTS (SELECT 1 FROM doc_tombstones t WHERE t.doc_id = d.id)`
	}
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func hasGraphQueryData(ctx context.Context, db *sql.DB) bool {
	if err := ensureGraphQueryReady(ctx, db); err != nil {
		return false
	}
	return true
}

func sourceGraphTablesReady(ctx context.Context, db *sql.DB) (bool, error) {
	for _, table := range []string{"entities", "edges", "doc_entities"} {
		ok, err := attachedTableExists(ctx, db, "src", table)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func attachedTableExists(ctx context.Context, db *sql.DB, alias, table string) (bool, error) {
	query := fmt.Sprintf("SELECT 1 FROM %s.%s LIMIT 1", alias, table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "does not exist") || strings.Contains(msg, "not found") {
			return false, nil
		}
		return false, err
	}
	return true, rows.Close()
}

func quoteSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
