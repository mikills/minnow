package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	kb "github.com/mikills/minnow/kb"
)

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
	expectedDim, err := duckDBEmbeddingDimension(ctx, db)
	if err != nil {
		return kb.IngestResult{}, err
	}
	if err := validatePreparedDocDimensions(preparedDocs, expectedDim, "upsert embedding dimension is incompatible with stored vectors"); err != nil {
		return kb.IngestResult{}, err
	}

	if err := f.applyUpsert(ctx, db, preparedDocs, graphBuilder); err != nil {
		return kb.IngestResult{}, err
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if err := f.postMutationCommit(ctx, db, req.KBID, req.Upload, policy.TargetShardBytes); err != nil {
		return kb.IngestResult{}, err
	}
	if req.Upload {
		if err := f.cleanupPreShardSnapshotObjectsBestEffort(ctx, req.KBID); err != nil {
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

func (f *DuckDBArtifactFormat) PublishPrepared(ctx context.Context, req kb.PreparedPublishRequest) (kb.IngestResult, error) {
	if req.KBID == "" {
		return kb.IngestResult{}, fmt.Errorf("kbID cannot be empty")
	}
	if len(req.Docs) == 0 {
		return kb.IngestResult{}, nil
	}

	preparedDocs, err := embeddedToPreparedDocs(req.Docs)
	if err != nil {
		return kb.IngestResult{}, err
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
	expectedDim, err := duckDBEmbeddingDimension(ctx, db)
	if err != nil {
		return kb.IngestResult{}, err
	}
	if err := validatePreparedDocDimensions(preparedDocs, expectedDim, "prepared embedding dimension is incompatible with stored vectors"); err != nil {
		return kb.IngestResult{}, err
	}

	if err := f.applyPreparedUpsert(ctx, db, preparedDocs, req.GraphResult); err != nil {
		return kb.IngestResult{}, err
	}

	policy := kb.NormalizeShardingPolicy(f.deps.ShardingPolicy)
	if err := f.postMutationCommitWithRetry(ctx, db, req.KBID, req.Upload, policy.TargetShardBytes, 5); err != nil {
		return kb.IngestResult{}, err
	}
	if req.Upload {
		if err := f.cleanupPreShardSnapshotObjectsBestEffort(ctx, req.KBID); err != nil {
			return kb.IngestResult{}, err
		}
	}

	return kb.IngestResult{MutatedCount: len(req.Docs)}, nil
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
			embedding FLOAT[` + strconv.Itoa(embeddingDim) + `],
			media_refs TEXT
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
	return f.postMutationCommitWithRetry(ctx, db, kbID, upload, targetShardBytes, 1)
}

func (f *DuckDBArtifactFormat) postMutationCommitWithRetry(ctx context.Context, db *sql.DB, kbID string, upload bool, targetShardBytes int64, maxAttempts int) error {
	kbDir := filepath.Join(f.deps.CacheDir, kbID)
	dbPath := filepath.Join(kbDir, "vectors.duckdb")

	if err := checkpointAndCloseMutationDB(ctx, db); err != nil {
		return err
	}

	if upload {
		if err := f.publishMutationSnapshotWithBackoff(ctx, kbID, kbDir, dbPath, targetShardBytes, maxAttempts); err != nil {
			return err
		}
	}

	if err := f.deps.EvictCacheIfNeeded(ctx, kbID); err != nil {
		return err
	}

	return nil
}

func checkpointAndCloseMutationDB(ctx context.Context, db *sql.DB) error {
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
	return nil
}

func (f *DuckDBArtifactFormat) publishMutationSnapshotWithBackoff(ctx context.Context, kbID, kbDir, dbPath string, targetShardBytes int64, maxAttempts int) error {
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	const (
		transientMaxAttempts = 5
		transientBaseDelay   = 100 * time.Millisecond
		transientCapDelay    = 2 * time.Second
	)
	var lastOrphanedKeys []string
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		expectedManifestVersion, err := f.deps.ManifestStore.HeadVersion(ctx, kbID)
		if err != nil {
			if isTransientBlobError(err) && attempt < maxAttempts {
				sleepBackoff(attempt, transientBaseDelay, transientCapDelay)
				continue
			}
			return err
		}

		artifacts, err := f.BuildArtifacts(ctx, kbID, dbPath, targetShardBytes)
		if err != nil {
			return err
		}
		lastOrphanedKeys = lastOrphanedKeys[:0]
		for _, a := range artifacts {
			lastOrphanedKeys = append(lastOrphanedKeys, a.Key)
		}
		if auditErr := f.auditStagedShardBlobs(ctx, artifacts); auditErr != nil {
			if isTransientBlobError(auditErr) && attempt < maxAttempts {
				sleepBackoff(attempt, transientBaseDelay, transientCapDelay)
				continue
			}
			return auditErr
		}

		newVersion, err := f.publishMutationManifest(ctx, kbID, artifacts, expectedManifestVersion)
		if err != nil {
			shouldRetry := errors.Is(err, kb.ErrBlobVersionMismatch) || isTransientBlobError(err)
			if attempt >= maxAttempts && attempt >= transientMaxAttempts {
				logOrphanedShardBlobs(ctx, kbID, attempt, err, lastOrphanedKeys)
				return err
			}
			if !shouldRetry {
				return err
			}
			// Allow more attempts for transient blob errors than the CAS-only
			// path by extending maxAttempts when triggered by network shape.
			if isTransientBlobError(err) && attempt >= maxAttempts && attempt < transientMaxAttempts {
				sleepBackoff(attempt, transientBaseDelay, transientCapDelay)
				// Bump attempt ceiling so we keep retrying up to
				// transientMaxAttempts on network-shaped errors.
				maxAttempts = transientMaxAttempts
				continue
			}
			sleepBackoff(attempt, transientBaseDelay, transientCapDelay)
			continue
		}
		f.deps.Metrics.RecordShardCount(kbID, len(artifacts))
		if err := writeLocalShardManifestVersion(localShardManifestVersionPath(kbDir), newVersion); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (f *DuckDBArtifactFormat) auditStagedShardBlobs(ctx context.Context, artifacts []kb.SnapshotShardMetadata) error {
	for _, a := range artifacts {
		if _, err := f.deps.BlobStore.Head(ctx, a.Key); err != nil {
			return fmt.Errorf("audit staged artifact %s: %w", a.Key, err)
		}
	}
	return nil
}

func (f *DuckDBArtifactFormat) publishMutationManifest(ctx context.Context, kbID string, artifacts []kb.SnapshotShardMetadata, expectedManifestVersion string) (string, error) {
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
	return f.deps.ManifestStore.UpsertIfMatch(ctx, kbID, manifest, expectedManifestVersion)
}

func logOrphanedShardBlobs(ctx context.Context, kbID string, attempt int, err error, orphanedKeys []string) {
	slog.Default().WarnContext(ctx, "manifest publish failed; orphaned shard blobs may exist",
		"kb_id", kbID,
		"attempts", attempt,
		"error", err,
		"orphaned_keys", orphanedKeys,
	)
}

// isTransientBlobError reports whether err looks like a retriable
// network/blob error. context.DeadlineExceeded is deliberately excluded;
// that's a caller-imposed timeout, and retrying inside the same context
// just wastes work.
func isTransientBlobError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	msg := strings.ToLower(err.Error())
	transientMarkers := []string{
		"connection reset",
		"i/o timeout",
		"temporary failure",
		"connection refused",
		"no such host",
		"broken pipe",
		"eof",
		"unexpected eof",
	}
	for _, marker := range transientMarkers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func sleepBackoff(attempt int, base, cap time.Duration) {
	if attempt < 1 {
		attempt = 1
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= cap {
			d = cap
			break
		}
	}
	time.Sleep(d)
}

type preparedUpsertDoc struct {
	Doc       kb.Document
	Embedding []float32
}

func embeddedToPreparedDocs(docs []kb.EmbeddedDocument) ([]preparedUpsertDoc, error) {
	prepared := make([]preparedUpsertDoc, 0, len(docs))
	for _, doc := range docs {
		if strings.TrimSpace(doc.ID) == "" {
			return nil, fmt.Errorf("doc id cannot be empty")
		}
		if strings.TrimSpace(doc.Text) == "" {
			return nil, fmt.Errorf("doc %q text cannot be empty", doc.ID)
		}
		if len(doc.Embedding) == 0 {
			return nil, fmt.Errorf("doc %q embedding cannot be empty", doc.ID)
		}
		prepared = append(prepared, preparedUpsertDoc{
			Doc: kb.Document{
				ID:        doc.ID,
				Text:      doc.Text,
				MediaIDs:  doc.MediaIDs,
				MediaRefs: doc.MediaRefs,
				Metadata:  doc.Metadata,
			},
			Embedding: doc.Embedding,
		})
	}
	return prepared, nil
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
	return applyPreparedDocsTx(ctx, tx, docs, graphResult, graphResult != nil)
}

func (f *DuckDBArtifactFormat) applyPreparedUpsert(ctx context.Context, db *sql.DB, docs []preparedUpsertDoc, graphResult *kb.GraphBuildResult) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert tx: %w", err)
	}
	return applyPreparedDocsTx(ctx, tx, docs, graphResult, graphResult != nil)
}

func applyPreparedDocsTx(ctx context.Context, tx *sql.Tx, docs []preparedUpsertDoc, graphResult *kb.GraphBuildResult, ensureGraphTables bool) error {
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
		mediaRefsJSON, err := encodeMediaRefs(doc.MediaIDs, doc.MediaRefs)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("encode media refs for doc %q: %w", doc.ID, err)
		}
		upsertSQL := fmt.Sprintf(`INSERT INTO docs (id, content, embedding, media_refs) VALUES (?, ?, %s::FLOAT[%d], ?)`, vecStr, len(vec))
		if _, err := tx.ExecContext(ctx, upsertSQL, doc.ID, doc.Text, mediaRefsJSON); err != nil {
			tx.Rollback()
			return kb.WrapEmbeddingDimensionMismatch(fmt.Errorf("upsert doc %q: %w", doc.ID, err), "upsert embedding dimension is incompatible with stored vectors")
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
		if ensureGraphTables {
			if err := ensureGraphTablesForTx(ctx, tx); err != nil {
				tx.Rollback()
				return err
			}
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

func ensureGraphTablesForTx(ctx context.Context, tx *sql.Tx) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS entities (id TEXT PRIMARY KEY, name TEXT)`,
		`CREATE TABLE IF NOT EXISTS edges (src TEXT, dst TEXT, weight DOUBLE, rel_type TEXT, chunk_id TEXT)`,
		`CREATE TABLE IF NOT EXISTS doc_entities (doc_id TEXT, entity_id TEXT, weight DOUBLE, chunk_id TEXT)`,
		`CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src)`,
		`CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges(dst)`,
		`CREATE INDEX IF NOT EXISTS idx_doc_entities_doc_id ON doc_entities(doc_id)`,
		`CREATE INDEX IF NOT EXISTS idx_doc_entities_entity_id ON doc_entities(entity_id)`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure graph tables: %w", err)
		}
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
