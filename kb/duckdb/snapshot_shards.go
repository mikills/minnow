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
		part, vectorRows, nextDocID, err := f.buildAndUploadOneSnapshotShard(ctx, prefix, localDBPath, hasTombstones, shardPath, shardIndex, rowsPerShard, lastDocID)
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

func (f *DuckDBArtifactFormat) buildAndUploadOneSnapshotShard(ctx context.Context, prefix, localDBPath string, hasTombstones bool, shardPath string, shardIndex, rowsPerShard int, lastDocID string) (kb.SnapshotShardMetadata, int64, string, error) {
	vectorRows, graphAvailable, nextDocID, centroid, err := f.buildShardDBFromSourceRange(ctx, localDBPath, hasTombstones, shardPath, rowsPerShard, lastDocID)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	if vectorRows <= 0 {
		return kb.SnapshotShardMetadata{}, 0, nextDocID, nil
	}

	sha, err := kb.FileContentSHA256(shardPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	info, err := os.Stat(shardPath)
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}
	partKey := fmt.Sprintf("%s/shard-%05d.duckdb", prefix, shardIndex)
	uploadInfo, err := f.deps.BlobStore.UploadIfMatch(ctx, partKey, shardPath, "")
	if err != nil {
		return kb.SnapshotShardMetadata{}, 0, "", err
	}

	now := time.Now().UTC()
	mediaIDs, mediaErr := collectShardMediaIDs(ctx, shardPath, f.openConfiguredDB)
	if mediaErr != nil {
		return kb.SnapshotShardMetadata{}, 0, "", fmt.Errorf("collect shard media ids: %w", mediaErr)
	}
	return kb.SnapshotShardMetadata{
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
		MediaIDs:       mediaIDs,
	}, vectorRows, nextDocID, nil
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

	if err := copyShardRowRange(ctx, shardDB, hasTombstones, limit, lastDocID); err != nil {
		return 0, false, "", nil, err
	}

	if _, err := shardDB.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS docs_vec_idx ON docs USING HNSW (embedding)`); err != nil {
		return 0, false, "", nil, err
	}

	if err := copyShardGraphTables(ctx, shardDB); err != nil {
		return 0, false, "", nil, err
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
	// NOTE: SnapshotShardMetadata.MediaIDs is populated at manifest-publish
	// time by walking docs.media_refs; see collectShardMediaIDs.
	if err := CheckpointAndCloseDB(ctx, shardDB, "close shard db"); err != nil {
		return 0, false, "", nil, err
	}
	return rowCount, graphAvailable, maxDocID, centroid, nil
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

	tmpDir, err := os.MkdirTemp("", "minnow-shard-download-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	tmpFile, err := os.CreateTemp(filepath.Dir(dest), "minnow-reconstruct-*.duckdb")
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

// DownloadSnapshotFromShards downloads and reconstructs a sharded snapshot into a single DB file.
func (f *DuckDBArtifactFormat) DownloadSnapshotFromShards(ctx context.Context, kbID, dest string) (*kb.SnapshotShardManifest, error) {
	return f.downloadSnapshotFromShards(ctx, kbID, dest)
}

// cleanupPreShardSnapshotObjectsBestEffort deletes the monolithic snapshot
// objects from the pre-sharded layout that may still exist in long-running
// deployments. Failures only log; the upload path must succeed regardless.
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
	// Safe: alias is an internal-only constant; never accept from user input.
	// If this changes, switch to a sanitized allowlist.
	AssertSafeIdentifier(alias)
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH '%s' AS %s (READ_ONLY)", quoteSQLString(partPath), alias)); err != nil {
		return err
	}
	defer func() { _, _ = db.ExecContext(context.Background(), fmt.Sprintf("DETACH %s", alias)) }()

	if isFirst {
		// Safe: alias + fixed column list interpolation only.
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE docs AS SELECT id, content, embedding, media_refs FROM %s.docs WHERE 1=0", alias)); err != nil {
			return err
		}
		if err := EnsureGraphTables(ctx, db); err != nil {
			return err
		}
	}
	// Safe: alias + fixed column list interpolation only.
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO docs (id, content, embedding, media_refs) SELECT id, content, embedding, media_refs FROM %s.docs", alias)); err != nil {
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
	// Safe: alias and table are internal-only constants; never accept from
	// user input. If this changes, switch to a sanitized allowlist.
	AssertSafeIdentifier(alias)
	AssertSafeIdentifier(table)
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
