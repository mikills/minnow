package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"

	kb "github.com/mikills/minnow/kb"
)

var tempTableCounter atomic.Int64

func uniqueTempName(prefix string) string {
	n := tempTableCounter.Add(1)
	return fmt.Sprintf("%s_%d", prefix, n)
}

const tempTableThreshold = 200

func ensureEntityGraphTables(ctx context.Context, db *sql.DB) error {
	if err := ensureEdgesTable(ctx, db); err != nil {
		return err
	}
	if err := ensureDocEntitiesTable(ctx, db); err != nil {
		return err
	}
	return nil
}

// EnsureGraphTables creates the entities, edges, and doc_entities tables and
// their associated indexes if they do not already exist.
func EnsureGraphTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS entities (
			id TEXT PRIMARY KEY,
			name TEXT
		)
	`); err != nil {
		return fmt.Errorf("create entities table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS edges (
			src TEXT,
			dst TEXT,
			weight DOUBLE,
			rel_type TEXT,
			chunk_id TEXT
		)
	`); err != nil {
		return fmt.Errorf("create edges table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS doc_entities (
			doc_id TEXT,
			entity_id TEXT,
			weight DOUBLE,
			chunk_id TEXT
		)
	`); err != nil {
		return fmt.Errorf("create doc_entities table: %w", err)
	}

	indexes := []struct {
		name string
		ddl  string
	}{
		{"idx_edges_src", `CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src)`},
		{"idx_edges_dst", `CREATE INDEX IF NOT EXISTS idx_edges_dst ON edges(dst)`},
		{"idx_doc_entities_doc_id", `CREATE INDEX IF NOT EXISTS idx_doc_entities_doc_id ON doc_entities(doc_id)`},
		{"idx_doc_entities_entity_id", `CREATE INDEX IF NOT EXISTS idx_doc_entities_entity_id ON doc_entities(entity_id)`},
	}
	for _, idx := range indexes {
		if _, err := db.ExecContext(ctx, idx.ddl); err != nil {
			return fmt.Errorf("create index %s: %w", idx.name, err)
		}
	}

	return nil
}

// InsertGraphBuildResult writes the full GraphBuildResult into db within a
// single transaction.
func InsertGraphBuildResult(ctx context.Context, db *sql.DB, result *kb.GraphBuildResult) error {
	if result == nil {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin graph insert tx: %w", err)
	}
	if err := InsertGraphBuildResultTx(ctx, tx, result); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit graph insert tx: %w", err)
	}
	return nil
}

// InsertGraphBuildResultTx writes entities, edges, and doc-entity links into
// the given transaction.
func InsertGraphBuildResultTx(ctx context.Context, tx *sql.Tx, result *kb.GraphBuildResult) error {
	if result == nil {
		return nil
	}

	if len(result.Entities) > 0 {
		var err error
		stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO entities (id, name) VALUES (?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare entity insert: %w", err)
		}
		for _, ent := range result.Entities {
			if ent.ID == "" {
				continue
			}
			if _, err := stmt.ExecContext(ctx, ent.ID, ent.Name); err != nil {
				stmt.Close()
				return fmt.Errorf("insert entity %s: %w", ent.ID, err)
			}
		}
		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close entity stmt: %w", err)
		}
	}

	if len(result.Edges) > 0 {
		var err error
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO edges (src, dst, weight, rel_type, chunk_id) VALUES (?, ?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare edge insert: %w", err)
		}
		for _, edge := range result.Edges {
			if edge.SrcID == "" || edge.DstID == "" {
				continue
			}
			weight := edge.Weight
			if weight <= 0 {
				weight = 1.0
			}
			if _, err := stmt.ExecContext(ctx, edge.SrcID, edge.DstID, weight, edge.RelType, edge.ChunkID); err != nil {
				stmt.Close()
				return fmt.Errorf("insert edge %s->%s: %w", edge.SrcID, edge.DstID, err)
			}
		}
		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close edge stmt: %w", err)
		}
	}

	chunkToDoc := make(map[string]string, len(result.Chunks))
	for _, chunk := range result.Chunks {
		if chunk.ChunkID == "" || chunk.DocID == "" {
			continue
		}
		chunkToDoc[chunk.ChunkID] = chunk.DocID
	}

	type docEntityKey struct {
		docID    string
		entityID string
		chunkID  string
	}
	docEntityWeights := make(map[docEntityKey]float64)

	for _, edge := range result.Edges {
		docID := chunkToDoc[edge.ChunkID]
		if docID == "" {
			continue
		}
		weight := edge.Weight
		if weight <= 0 {
			weight = 1.0
		}
		docEntityWeights[docEntityKey{docID: docID, entityID: edge.SrcID, chunkID: edge.ChunkID}] += weight
		docEntityWeights[docEntityKey{docID: docID, entityID: edge.DstID, chunkID: edge.ChunkID}] += weight
	}

	for _, m := range result.EntityChunkMappings {
		if m.EntityID == "" || m.ChunkID == "" {
			continue
		}
		docID := chunkToDoc[m.ChunkID]
		if docID == "" {
			continue
		}
		key := docEntityKey{docID: docID, entityID: m.EntityID, chunkID: m.ChunkID}
		if _, ok := docEntityWeights[key]; !ok {
			docEntityWeights[key] = 1.0
		}
	}

	if len(docEntityWeights) > 0 {
		var err error
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO doc_entities (doc_id, entity_id, weight, chunk_id) VALUES (?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("prepare doc_entity insert: %w", err)
		}
		for key, weight := range docEntityWeights {
			if _, err := stmt.ExecContext(ctx, key.docID, key.entityID, weight, key.chunkID); err != nil {
				stmt.Close()
				return fmt.Errorf("insert doc_entity %s->%s: %w", key.docID, key.entityID, err)
			}
		}
		if err := stmt.Close(); err != nil {
			return fmt.Errorf("close doc_entity stmt: %w", err)
		}
	}

	return nil
}

func ensureEdgesTable(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT 1 FROM edges LIMIT 0`)
	if err != nil {
		return fmt.Errorf("edges table missing: %w", err)
	}
	return rows.Close()
}

func ensureDocEntitiesTable(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT 1 FROM doc_entities LIMIT 0`)
	if err != nil {
		return fmt.Errorf("doc_entities table missing: %w", err)
	}
	return rows.Close()
}

func ensureEntitiesTable(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `SELECT 1 FROM entities LIMIT 0`)
	if err != nil {
		return fmt.Errorf("entities table missing: %w", err)
	}
	return rows.Close()
}

func queryEdgesBySources(ctx context.Context, db *sql.DB, sources []string, edgeTypes []string, maxNeighborsPerNode int) ([]kb.EdgeRow, error) {
	if len(sources) == 0 {
		return nil, nil
	}

	if shouldUseTempTable(len(sources)) {
		return queryEdgesBySourcesTempTable(ctx, db, sources, edgeTypes, maxNeighborsPerNode)
	}

	var sb strings.Builder
	srcPlaceholders := kb.BuildInClausePlaceholders(len(sources))

	args := make([]any, 0, 2*len(sources)+2*len(edgeTypes)+1)

	sb.WriteString(`
		WITH ranked AS (
			SELECT src, dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE src IN (`)
	sb.WriteString(srcPlaceholders)
	sb.WriteString(")")
	for _, source := range sources {
		args = append(args, source)
	}
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(kb.BuildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(")")
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}

	sb.WriteString(`
			UNION ALL
			SELECT dst AS src, src AS dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE dst IN (`)
	sb.WriteString(srcPlaceholders)
	sb.WriteString(")")
	for _, source := range sources {
		args = append(args, source)
	}
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(kb.BuildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(")")
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}

	sb.WriteString(`
		),
		limited AS (
			SELECT src, dst, weight,
				ROW_NUMBER() OVER (PARTITION BY src ORDER BY weight DESC) AS rn
			FROM ranked
		)
		SELECT src, dst, weight
		FROM limited
		WHERE rn <= ?
	`)
	args = append(args, maxNeighborsPerNode)

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("edge query failed: %w", err)
	}
	defer rows.Close()

	results := make([]kb.EdgeRow, 0, len(sources))
	for rows.Next() {
		var row kb.EdgeRow
		if err := rows.Scan(&row.Src, &row.Dst, &row.Weight); err != nil {
			return nil, fmt.Errorf("failed to scan edge row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edge rows iteration error: %w", err)
	}

	return results, nil
}

func queryEdgesBySourcesTempTable(ctx context.Context, db *sql.DB, sources []string, edgeTypes []string, maxNeighborsPerNode int) ([]kb.EdgeRow, error) {
	tempName := uniqueTempName("tmp_edge_sources")
	if err := createTempIDTable(ctx, db, tempName); err != nil {
		return nil, err
	}
	defer func() { _ = dropTable(ctx, db, tempName) }()

	if err := insertIDs(ctx, db, tempName, sources); err != nil {
		return nil, err
	}

	var sb strings.Builder
	args := make([]any, 0, 2*len(edgeTypes)+1)

	sb.WriteString(fmt.Sprintf(`
		WITH ranked AS (
			SELECT src, dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE src IN (SELECT id FROM %s)`, tempName))
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(kb.BuildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(`)`)
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}
	sb.WriteString(fmt.Sprintf(`
			UNION ALL
			SELECT dst AS src, src AS dst, COALESCE(weight, 1.0) AS weight
			FROM edges
			WHERE dst IN (SELECT id FROM %s)`, tempName))
	if len(edgeTypes) > 0 {
		sb.WriteString(` AND rel_type IN (`)
		sb.WriteString(kb.BuildInClausePlaceholders(len(edgeTypes)))
		sb.WriteString(`)`)
		for _, et := range edgeTypes {
			args = append(args, et)
		}
	}
	sb.WriteString(`
		),
		limited AS (
			SELECT src, dst, weight,
				ROW_NUMBER() OVER (PARTITION BY src ORDER BY weight DESC) AS rn
			FROM ranked
		)
		SELECT src, dst, weight
		FROM limited
		WHERE rn <= ?
	`)
	args = append(args, maxNeighborsPerNode)

	rows, err := db.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("edge query failed: %w", err)
	}
	defer rows.Close()

	results := make([]kb.EdgeRow, 0, len(sources))
	for rows.Next() {
		var row kb.EdgeRow
		if err := rows.Scan(&row.Src, &row.Dst, &row.Weight); err != nil {
			return nil, fmt.Errorf("failed to scan edge row: %w", err)
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("edge rows iteration error: %w", err)
	}

	return results, nil
}

func queryEntitiesForDocs(ctx context.Context, db *sql.DB, docIDs []string) (map[string]float64, error) {
	if len(docIDs) == 0 {
		return map[string]float64{}, nil
	}

	if shouldUseTempTable(len(docIDs)) {
		return queryEntitiesForDocsTempTable(ctx, db, docIDs)
	}

	placeholders := kb.BuildInClausePlaceholders(len(docIDs))
	query := fmt.Sprintf(`
		SELECT entity_id, COALESCE(weight, 1.0) AS weight
		FROM doc_entities
		WHERE doc_id IN (%s)
	`, placeholders)

	args := make([]any, 0, len(docIDs))
	for _, id := range docIDs {
		args = append(args, id)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("entity query failed: %w", err)
	}
	defer rows.Close()

	entities := make(map[string]float64)
	for rows.Next() {
		var id string
		var weight float64
		if err := rows.Scan(&id, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan entity: %w", err)
		}
		if id == "" {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		entities[id] += weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("entity rows iteration error: %w", err)
	}

	return entities, nil
}

func queryDocsForEntities(ctx context.Context, db *sql.DB, entityScores map[string]float64) (map[string]float64, error) {
	if len(entityScores) == 0 {
		return map[string]float64{}, nil
	}

	if shouldUseTempTable(len(entityScores)) {
		return queryDocsForEntitiesTempTable(ctx, db, entityScores)
	}

	entityIDs := kb.MapKeys(entityScores)
	placeholders := kb.BuildInClausePlaceholders(len(entityIDs))
	query := fmt.Sprintf(`
		SELECT doc_id, entity_id, COALESCE(weight, 1.0) AS weight
		FROM doc_entities
		WHERE entity_id IN (%s)
	`, placeholders)

	args := make([]any, 0, len(entityIDs))
	for _, id := range entityIDs {
		args = append(args, id)
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("doc_entities query failed: %w", err)
	}
	defer rows.Close()

	docScores := make(map[string]float64)
	for rows.Next() {
		var docID string
		var entityID string
		var weight float64
		if err := rows.Scan(&docID, &entityID, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan doc_entities row: %w", err)
		}
		entityScore := entityScores[entityID]
		if entityScore == 0 {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		docScores[docID] += entityScore * weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("doc_entities rows iteration error: %w", err)
	}

	return docScores, nil
}

func queryEntitiesForDocsTempTable(ctx context.Context, db *sql.DB, docIDs []string) (map[string]float64, error) {
	tempName := uniqueTempName("tmp_doc_ids")
	if err := createTempIDTable(ctx, db, tempName); err != nil {
		return nil, err
	}
	defer func() {
		_ = dropTable(ctx, db, tempName)
	}()

	if err := insertIDs(ctx, db, tempName, docIDs); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT de.entity_id, COALESCE(de.weight, 1.0) AS weight
		FROM doc_entities de
		INNER JOIN %s ids ON de.doc_id = ids.id
	`, tempName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("entity query failed: %w", err)
	}
	defer rows.Close()

	entities := make(map[string]float64)
	for rows.Next() {
		var id string
		var weight float64
		if err := rows.Scan(&id, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan entity: %w", err)
		}
		if id == "" {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		entities[id] += weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("entity rows iteration error: %w", err)
	}

	return entities, nil
}

func queryDocsForEntitiesTempTable(ctx context.Context, db *sql.DB, entityScores map[string]float64) (map[string]float64, error) {
	entityIDs := kb.MapKeys(entityScores)
	tempName := uniqueTempName("tmp_entity_ids")
	if err := createTempIDTable(ctx, db, tempName); err != nil {
		return nil, err
	}
	defer func() {
		_ = dropTable(ctx, db, tempName)
	}()

	if err := insertIDs(ctx, db, tempName, entityIDs); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT de.doc_id, de.entity_id, COALESCE(de.weight, 1.0) AS weight
		FROM doc_entities de
		INNER JOIN %s ids ON de.entity_id = ids.id
	`, tempName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("doc_entities query failed: %w", err)
	}
	defer rows.Close()

	docScores := make(map[string]float64)
	for rows.Next() {
		var docID string
		var entityID string
		var weight float64
		if err := rows.Scan(&docID, &entityID, &weight); err != nil {
			return nil, fmt.Errorf("failed to scan doc_entities row: %w", err)
		}
		entityScore := entityScores[entityID]
		if entityScore == 0 {
			continue
		}
		if weight <= 0 {
			weight = 1.0
		}
		docScores[docID] += entityScore * weight
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("doc_entities rows iteration error: %w", err)
	}

	return docScores, nil
}

func shouldUseTempTable(count int) bool {
	return count >= tempTableThreshold
}

func createTempIDTable(ctx context.Context, db *sql.DB, name string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TEMP TABLE %s (id TEXT)`, name)); err != nil {
		return fmt.Errorf("create temp id table: %w", err)
	}
	return nil
}

func insertIDs(ctx context.Context, db *sql.DB, tableName string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin id insert tx: %w", err)
	}
	stmt, err := tx.PrepareContext(ctx, fmt.Sprintf(`INSERT INTO %s (id) VALUES (?)`, tableName))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare id insert: %w", err)
	}
	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, id); err != nil {
			stmt.Close()
			tx.Rollback()
			return fmt.Errorf("insert id: %w", err)
		}
	}
	if err := stmt.Close(); err != nil {
		tx.Rollback()
		return fmt.Errorf("close id stmt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit id insert tx: %w", err)
	}
	return nil
}

func dropTable(ctx context.Context, db *sql.DB, tableName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	return err
}

func pruneGraphForDocsTx(ctx context.Context, tx *sql.Tx, docIDs []string, cleanupEdgesAndEntities bool) error {
	if len(docIDs) == 0 {
		return nil
	}

	hasDocEntities, err := tableExists(ctx, tx, "doc_entities")
	if err != nil {
		return err
	}
	if !hasDocEntities {
		return nil
	}

	placeholders := kb.BuildInClausePlaceholders(len(docIDs))
	args := make([]any, 0, len(docIDs))
	for _, id := range docIDs {
		args = append(args, id)
	}

	chunkIDs := make([]string, 0)
	if cleanupEdgesAndEntities {
		rows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT DISTINCT chunk_id FROM doc_entities WHERE doc_id IN (%s)`, placeholders), args...)
		if err != nil {
			return fmt.Errorf("query doc chunk ids: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var chunkID string
			if err := rows.Scan(&chunkID); err != nil {
				return fmt.Errorf("scan chunk id: %w", err)
			}
			if strings.TrimSpace(chunkID) != "" {
				chunkIDs = append(chunkIDs, chunkID)
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate chunk id rows: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM doc_entities WHERE doc_id IN (%s)`, placeholders), args...); err != nil {
		return fmt.Errorf("prune doc_entities: %w", err)
	}

	if !cleanupEdgesAndEntities {
		return nil
	}

	hasEdges, err := tableExists(ctx, tx, "edges")
	if err != nil {
		return err
	}
	if hasEdges && len(chunkIDs) > 0 {
		edgePlaceholders := kb.BuildInClausePlaceholders(len(chunkIDs))
		edgeArgs := make([]any, 0, len(chunkIDs))
		for _, id := range chunkIDs {
			edgeArgs = append(edgeArgs, id)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM edges WHERE chunk_id IN (%s)`, edgePlaceholders), edgeArgs...); err != nil {
			return fmt.Errorf("prune edges by chunk: %w", err)
		}
	}

	hasEntities, err := tableExists(ctx, tx, "entities")
	if err != nil {
		return err
	}
	if hasEntities {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM entities e
			WHERE NOT EXISTS (
				SELECT 1 FROM doc_entities de WHERE de.entity_id = e.id
			)
			AND NOT EXISTS (
				SELECT 1 FROM edges ed WHERE ed.src = e.id OR ed.dst = e.id
			)
		`); err != nil {
			return fmt.Errorf("prune orphan entities: %w", err)
		}
	}

	return nil
}
