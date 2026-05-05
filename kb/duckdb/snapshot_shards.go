package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	kb "github.com/mikills/minnow/kb"
)

func (f *DuckDBArtifactFormat) BuildArtifacts(ctx context.Context, kbID, srcPath string, targetBytes int64) ([]kb.SnapshotShardMetadata, error) {
	if targetBytes <= 0 {
		targetBytes = kb.DefaultSnapshotShardSize
	}
	dbHash, err := kb.FileContentSHA256(ctx, srcPath)
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

func (f *DuckDBArtifactFormat) uploadQueryableSnapshotShards(ctx context.Context, prefix, localDBPath string, partSize int64) ([]kb.SnapshotShardMetadata, int64, error) {
	hasTombstones, activeRows, err := f.inspectSnapshotSourceDB(ctx, localDBPath)
	if err != nil {
		return nil, 0, err
	}
	if activeRows <= 0 {
		return nil, 0, fmt.Errorf("cannot shard snapshot with zero active docs")
	}

	rowsPerShard, err := planShardRowCount(localDBPath, activeRows, partSize)
	if err != nil {
		return nil, 0, err
	}

	tmpDir, err := os.MkdirTemp("", "minnow-queryable-shards-*")
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
		part, vectorRows, nextDocID, err := f.buildAndUploadOneSnapshotShard(ctx, snapshotShardBuildInput{prefix: prefix, localDBPath: localDBPath, hasTombstones: hasTombstones, shardPath: shardPath, shardIndex: shardIndex, rowsPerShard: rowsPerShard, lastDocID: lastDocID})
		if err != nil {
			return nil, 0, err
		}
		if vectorRows <= 0 {
			return nil, 0, fmt.Errorf("failed to advance shard cursor while building snapshot shards")
		}
		lastDocID = nextDocID
		processedRows += vectorRows
		parts = append(parts, part)
		totalSize += part.SizeBytes
	}

	if len(parts) == 0 {
		return nil, 0, fmt.Errorf("failed to build shard files")
	}
	return parts, totalSize, nil
}

// inspectSnapshotSourceDB opens the source DB to detect tombstones and count
// active docs, then closes it before returning. DuckDB v1.5+ does not allow
// a file to be open by multiple database handles simultaneously, and shard
// building ATTACHes localDBPath from a separate handle.
func (f *DuckDBArtifactFormat) inspectSnapshotSourceDB(ctx context.Context, localDBPath string) (bool, int64, error) {
	sourceDB, err := f.openConfiguredDB(ctx, localDBPath)
	if err != nil {
		return false, 0, err
	}
	defer sourceDB.Close()

	hasTombstones, err := tableExists(ctx, sourceDB, "doc_tombstones")
	if err != nil {
		return false, 0, err
	}
	activeRows, err := countActiveDocs(ctx, sourceDB, hasTombstones)
	if err != nil {
		return false, 0, err
	}
	return hasTombstones, activeRows, nil
}

func planShardRowCount(localDBPath string, activeRows, partSize int64) (int, error) {
	sourceInfo, err := os.Stat(localDBPath)
	if err != nil {
		return 0, err
	}
	bytesPerRow := sourceInfo.Size() / activeRows
	if bytesPerRow <= 0 {
		bytesPerRow = 1
	}
	rowsPerShard := int(partSize / bytesPerRow)
	if rowsPerShard <= 0 {
		rowsPerShard = 1
	}
	return rowsPerShard, nil
}

type snapshotShardBuildInput struct {
	prefix        string
	localDBPath   string
	hasTombstones bool
	shardPath     string
	shardIndex    int
	rowsPerShard  int
	lastDocID     string
}

func (f *DuckDBArtifactFormat) buildAndUploadOneSnapshotShard(ctx context.Context, input snapshotShardBuildInput) (kb.SnapshotShardMetadata, int64, string, error) {
	vectorRows, graphAvailable, nextDocID, centroid, err := f.buildShardDBFromSourceRange(ctx, shardSourceRange{sourceDBPath: input.localDBPath, hasTombstones: input.hasTombstones, shardPath: input.shardPath, limit: input.rowsPerShard, lastDocID: input.lastDocID})
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	if vectorRows <= 0 {
		return kb.SnapshotShardMetadata{}, 0, nextDocID, nil
	}

	sha, err := kb.FileContentSHA256(ctx, input.shardPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	info, err := os.Stat(input.shardPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	partKey := fmt.Sprintf("%s/shard-%05d.duckdb", input.prefix, input.shardIndex)
	uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, partKey, input.shardPath, "")
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}

	now := time.Now().UTC()
	mediaIDs, mediaErr := collectShardMediaIDs(ctx, input.shardPath, f.openConfiguredDB)
	if mediaErr != nil {
		return kb.SnapshotShardMetadata{}, 0, "", fmt.Errorf("collect shard media ids: %w", mediaErr)
	}
	return kb.SnapshotShardMetadata{
		ShardID:        fmt.Sprintf("shard-%05d", input.shardIndex),
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
		MediaIDs:       mediaIDs,
	}, vectorRows, nextDocID, nil
}

type shardSourceRange struct {
	sourceDBPath  string
	hasTombstones bool
	shardPath     string
	limit         int
	lastDocID     string
}

func (f *DuckDBArtifactFormat) buildShardDBFromSourceRange(ctx context.Context, source shardSourceRange) (int64, bool, string, []float32, error) {
	if err := validateShardBuildLimit(source.limit); err != nil {
		return 0, false, "", nil, err
	}
	shardDB, err := f.openAttachedShardDB(ctx, source.sourceDBPath, source.shardPath)
	if err != nil {
		return 0, false, "", nil, err
	}
	defer shardDB.Close()
	defer detachSourceDB(ctx, shardDB)

	if err := copyShardRowRange(ctx, shardDB, source.hasTombstones, source.limit, source.lastDocID); err != nil {
		return 0, false, "", nil, err
	}

	if err := createDocsVectorIndex(ctx, shardDB); err != nil {
		return 0, false, "", nil, err
	}

	if err := copyShardGraphTables(ctx, shardDB); err != nil {
		return 0, false, "", nil, err
	}

	rowCount, empty, err := shardRowCountOrClose(ctx, shardDB)
	if err != nil || empty {
		return rowCount, false, source.lastDocID, nil, err
	}
	maxDocID, err := maxShardDocID(ctx, shardDB)
	if err != nil {
		return 0, false, "", nil, err
	}
	graphAvailable, centroid, err := finalizeBuiltShardDB(ctx, shardDB)
	if err != nil {
		return 0, false, "", nil, err
	}
	return rowCount, graphAvailable, maxDocID, centroid, nil
}

func finalizeBuiltShardDB(ctx context.Context, db *sql.DB) (bool, []float32, error) {
	centroid, err := computeShardCentroid(ctx, db)
	if err != nil {
		return false, nil, err
	}
	graphAvailable := hasGraphQueryData(ctx, db)
	// NOTE: SnapshotShardMetadata.MediaIDs is populated at manifest-publish
	// time by walking docs.media_refs. See collectShardMediaIDs.
	if err := CheckpointAndCloseDB(ctx, db, "close shard db"); err != nil {
		return false, nil, err
	}
	return graphAvailable, centroid, nil
}

func shardRowCountOrClose(ctx context.Context, db *sql.DB) (int64, bool, error) {
	rowCount, err := countCompactedDocs(ctx, db)
	if err != nil || rowCount > 0 {
		return rowCount, false, err
	}
	return 0, true, CheckpointAndCloseDB(ctx, db, "close empty shard db")
}

func maxShardDocID(ctx context.Context, db *sql.DB) (string, error) {
	var maxDocID string
	if err := db.QueryRowContext(ctx, `SELECT MAX(id) FROM docs`).Scan(&maxDocID); err != nil {
		return "", err
	}
	return maxDocID, nil
}

func validateShardBuildLimit(limit int) error {
	if limit <= 0 {
		return fmt.Errorf("limit must be > 0")
	}
	return nil
}

func (f *DuckDBArtifactFormat) openAttachedShardDB(ctx context.Context, sourceDBPath string, shardPath string) (*sql.DB, error) {
	shardDB, err := f.openConfiguredDB(ctx, shardPath)
	if err != nil {
		return nil, err
	}
	if _, err := shardDB.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS src (READ_ONLY)", quoteSQLString(sourceDBPath))); err != nil {
		_ = shardDB.Close()
		return nil, err
	}
	return shardDB, nil
}

func detachSourceDB(ctx context.Context, db *sql.DB) {
	_, _ = db.ExecContext(context.WithoutCancel(ctx), "DETACH src")
}

func copyShardRowRange(ctx context.Context, shardDB *sql.DB, hasTombstones bool, limit int, lastDocID string) error {
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
		SELECT d.id, d.content, d.embedding, d.media_refs
		FROM src.docs d
		%s
		ORDER BY d.id
		LIMIT %d
	`, where, limit)
	if _, err := shardDB.ExecContext(ctx, createSQL); err != nil {
		return err
	}
	return nil
}

func copyShardGraphTables(ctx context.Context, shardDB *sql.DB) error {
	if err := EnsureGraphTables(ctx, shardDB); err != nil {
		return err
	}

	ok, err := sourceGraphTablesReady(ctx, shardDB)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if _, err := shardDB.ExecContext(ctx, `
		INSERT INTO doc_entities (doc_id, entity_id, weight, chunk_id)
		SELECT de.doc_id, de.entity_id, de.weight, de.chunk_id
		FROM src.doc_entities de
		JOIN docs d ON d.id = de.doc_id
	`); err != nil {
		return err
	}
	if _, err := shardDB.ExecContext(ctx, `
		INSERT OR IGNORE INTO entities (id, name)
		SELECT DISTINCT e.id, e.name
		FROM src.entities e
		JOIN doc_entities de ON de.entity_id = e.id
	`); err != nil {
		return err
	}
	if _, err := shardDB.ExecContext(ctx, `
		INSERT INTO edges (src, dst, weight, rel_type, chunk_id)
		SELECT DISTINCT e.src, e.dst, e.weight, e.rel_type, e.chunk_id
		FROM src.edges e
		JOIN doc_entities de ON de.chunk_id = e.chunk_id
	`); err != nil {
		return err
	}
	return nil
}

// collectShardMediaIDs scans docs.media_refs for a shard file and returns
// the unique media ids it references. Called when sealing a shard so the
// manifest can carry the ids for pending-media GC promotion.
func collectShardMediaIDs(ctx context.Context, shardPath string, openDB func(context.Context, string) (*sql.DB, error)) ([]string, error) {
	db, err := openDB(ctx, shardPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT media_refs FROM docs WHERE media_refs IS NOT NULL AND media_refs != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for rows.Next() {
		var raw sql.NullString
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		refs, err := decodeMediaRefs(raw)
		if err != nil {
			continue
		}
		for _, ref := range refs {
			if ref.MediaID == "" {
				continue
			}
			if _, ok := seen[ref.MediaID]; ok {
				continue
			}
			seen[ref.MediaID] = struct{}{}
			out = append(out, ref.MediaID)
		}
	}
	return out, rows.Err()
}

func (f *DuckDBArtifactFormat) downloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*kb.SnapshotShardManifest, error) {
	if err := validateSnapshotDownload(kbID, dest); err != nil {
		return nil, err
	}

	manifest, err := f.downloadSourceManifest(ctx, kbID)
	if err != nil {
		return nil, err
	}

	tmpDir, err := os.MkdirTemp("", "minnow-shard-download-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	tmpDest, cleanupTmpDest, err := tempReconstructPath(dest)
	if err != nil {
		return nil, err
	}
	defer cleanupTmpDest()

	db, err := f.openConfiguredDB(ctx, tmpDest)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err := f.mergeManifestShards(ctx, db, tmpDir, manifest.Shards); err != nil {
		return nil, err
	}
	if err := finalizeReconstructedSnapshot(ctx, db); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpDest, dest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

func validateSnapshotDownload(kbID string, dest string) error {
	if kbID == "" {
		return fmt.Errorf("kbID cannot be empty")
	}
	if dest == "" {
		return fmt.Errorf("dest cannot be empty")
	}
	return nil
}

func (f *DuckDBArtifactFormat) downloadSourceManifest(ctx context.Context, kbID string) (kb.SnapshotShardManifest, error) {
	doc, err := f.deps.ManifestStore.Get(ctx, kbID)
	if err != nil {
		return kb.SnapshotShardManifest{}, err
	}
	manifest := doc.Manifest
	if err := f.validateManifestFormat(&manifest); err != nil {
		return kb.SnapshotShardManifest{}, err
	}
	f.deps.Metrics.RecordShardCount(kbID, len(manifest.Shards))
	if len(manifest.Shards) == 0 {
		return kb.SnapshotShardManifest{}, fmt.Errorf("manifest has no shards")
	}
	return manifest, nil
}

func tempReconstructPath(dest string) (string, func(), error) {
	tmpFile, err := os.CreateTemp(filepath.Dir(dest), "minnow-reconstruct-*.duckdb")
	if err != nil {
		return "", nil, err
	}
	tmpDest := tmpFile.Name()
	_ = tmpFile.Close()
	_ = os.Remove(tmpDest)
	return tmpDest, func() { _ = os.Remove(tmpDest) }, nil
}

func (f *DuckDBArtifactFormat) mergeManifestShards(ctx context.Context, db *sql.DB, tmpDir string, shards []kb.SnapshotShardMetadata) error {
	for i, shard := range shards {
		partPath := filepath.Join(tmpDir, fmt.Sprintf("part-%05d.duckdb", i))
		if err := f.downloadAndVerifyShardPart(ctx, shard, partPath); err != nil {
			return err
		}
		if err := mergeShardIntoDB(ctx, db, fmt.Sprintf("s%d", i), partPath, i == 0); err != nil {
			return err
		}
	}
	return nil
}

func (f *DuckDBArtifactFormat) downloadAndVerifyShardPart(ctx context.Context, shard kb.SnapshotShardMetadata, partPath string) error {
	if err := f.deps.BlobStore.Download(ctx, shard.Key, partPath); err != nil {
		return err
	}
	info, err := os.Stat(partPath)
	if err != nil {
		return err
	}
	if shard.SizeBytes > 0 && info.Size() != shard.SizeBytes {
		return fmt.Errorf("shard %s size mismatch", shard.ShardID)
	}
	if shard.SHA256 == "" {
		return nil
	}
	sha, err := kb.FileContentSHA256(ctx, partPath)
	if err != nil {
		return err
	}
	if sha != shard.SHA256 {
		return fmt.Errorf("shard %s checksum mismatch", shard.ShardID)
	}
	return nil
}

func finalizeReconstructedSnapshot(ctx context.Context, db *sql.DB) error {
	if err := ensureDocTombstonesTable(ctx, db); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return err
	}
	return CheckpointAndCloseDB(ctx, db, "close reconstructed shard snapshot")
}

// DownloadSnapshotFromShards downloads and reconstructs a sharded snapshot into a single DB file.
func (f *DuckDBArtifactFormat) DownloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*kb.SnapshotShardManifest, error) {
	return f.downloadSnapshotFromShards(ctx, kbID, dest)
}

// cleanupPreShardSnapshotObjectsBestEffort deletes the monolithic snapshot
// objects from the pre-sharded layout that may still exist in long-running
// deployments. Failures only log. the upload path must succeed regardless.
func (f *DuckDBArtifactFormat) cleanupPreShardSnapshotObjectsBestEffort(ctx context.Context, kbID string) error {
	preShardSnapshotKeys := []string{
		kbID + ".duckdb",
		kbID + ".snapshot.json",
	}
	for _, key := range preShardSnapshotKeys {
		err := f.deps.BlobStore.Delete(ctx, key)
		if err == nil || errors.Is(err, kb.ErrBlobNotFound) || errors.Is(err, os.ErrNotExist) {
			continue
		}
		slog.Default().WarnContext(ctx, "failed to delete pre-shard snapshot object", "kb_id", kbID, "key", key, "error", err)
	}
	return nil
}

// mergeShardIntoDB ATTACHes a shard file under alias, copies docs and graph
// tables into db, then DETACHes via defer. isFirst signals that the target
// tables must be created before inserting.
func mergeShardIntoDB(ctx context.Context, db *sql.DB, alias, partPath string, isFirst bool) error {
	if err := attachShardDB(ctx, db, alias, partPath); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf("DETACH %s", alias)) }()
	if isFirst {
		if err := initializeMergedShardTables(ctx, db, alias); err != nil {
			return err
		}
	}
	return copyShardTables(ctx, db, alias)
}

func attachShardDB(ctx context.Context, db *sql.DB, alias, partPath string) error {
	if err := ValidateSafeIdentifier(alias); err != nil {
		return err
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", quoteSQLString(partPath), alias))
	return err
}

func initializeMergedShardTables(ctx context.Context, db *sql.DB, alias string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE docs AS SELECT id, content, embedding, media_refs FROM %s.docs WHERE 1=0", alias)); err != nil {
		return err
	}
	return EnsureGraphTables(ctx, db)
}

func copyShardTables(ctx context.Context, db *sql.DB, alias string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO docs (id, content, embedding, media_refs) SELECT id, content, embedding, media_refs FROM %s.docs", alias)); err != nil {
		return err
	}
	for _, table := range []string{"doc_entities", "edges"} {
		if err := copyAttachedTableIfExists(ctx, db, alias, table, false); err != nil {
			return err
		}
	}
	return copyAttachedTableIfExists(ctx, db, alias, "entities", true)
}

func copyAttachedTableIfExists(ctx context.Context, db *sql.DB, alias string, table string, ignoreConflicts bool) error {
	ok, err := attachedTableExists(ctx, db, alias, table)
	if err != nil || !ok {
		return err
	}
	verb := "INSERT INTO"
	if ignoreConflicts {
		verb = "INSERT OR IGNORE INTO"
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf("%s %s SELECT * FROM %s.%s", verb, table, alias, table))
	return err
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
	// Safe: alias and table are internal-only constants. never accept from
	// user input. If this changes, switch to a sanitized allowlist.
	if err := ValidateSafeIdentifier(alias); err != nil {
		return false, err
	}
	if err := ValidateSafeIdentifier(table); err != nil {
		return false, err
	}
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

func computeShardCentroid(ctx context.Context, db *sql.DB) ([]float32, error) {
	rows, err := db.QueryContext(ctx, `SELECT CAST(embedding AS VARCHAR) FROM docs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sums, count, err := scanCentroidSums(rows)
	if err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 || len(sums) == 0 {
		return nil, nil
	}
	return centroidFromSums(sums, count), nil
}

func scanCentroidSums(rows *sql.Rows) ([]float64, int, error) {
	var sums []float64
	count := 0
	for rows.Next() {
		vec, err := scanCentroidVector(rows)
		if err != nil {
			return nil, 0, err
		}
		if len(vec) == 0 {
			continue
		}
		if err := addCentroidVector(&sums, vec); err != nil {
			return nil, 0, err
		}
		count++
	}
	return sums, count, nil
}

func scanCentroidVector(rows *sql.Rows) ([]float32, error) {
	var vecStr string
	if err := rows.Scan(&vecStr); err != nil {
		return nil, err
	}
	return parseDuckDBVectorString(vecStr)
}

func addCentroidVector(sums *[]float64, vec []float32) error {
	if *sums == nil {
		*sums = make([]float64, len(vec))
	}
	if len(vec) != len(*sums) {
		return fmt.Errorf("inconsistent embedding dimensions while computing centroid")
	}
	for i := range vec {
		(*sums)[i] += float64(vec[i])
	}
	return nil
}

func centroidFromSums(sums []float64, count int) []float32 {
	centroid := make([]float32, len(sums))
	for i := range sums {
		centroid[i] = float32(sums[i] / float64(count))
	}
	return centroid
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
