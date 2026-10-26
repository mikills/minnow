package duckdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// Only pooled, immutable shards use this cache. The caller holds shardConn.mu
// until returned rows have been consumed/closed. Mutable/direct DB helpers stay
// uncached, and filtered/scoped queries retain their existing literal SQL path.
const maxShardVectorStatements = 8

type shardVectorStatement struct {
	query string
	stmt  *sql.Stmt
}

func (sc *shardConn) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return sc.db.QueryContext(ctx, query, args...)
}

func (sc *shardConn) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return sc.db.QueryRowContext(ctx, query, args...)
}

func (sc *shardConn) metadataSelectExpr(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if sc.metadataExpr != "" {
		return sc.metadataExpr, nil
	}
	expr, err := docsMetadataSelectExpr(ctx, sc.db)
	if err != nil {
		return "", err // Never cache an interrupted/failed schema inspection.
	}
	sc.metadataExpr = expr
	return expr, nil
}

func (sc *shardConn) queryPreparedVector(ctx context.Context, query string, vector []float32) (*sql.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Tiny, bounded LRU: entries run from least to most recently used. SQL
	// includes projection, dimension and k, but never the changing vector.
	var stmt *sql.Stmt
	for i, entry := range sc.vectorStatements {
		if entry.query == query {
			stmt = entry.stmt
			copy(sc.vectorStatements[i:], sc.vectorStatements[i+1:])
			sc.vectorStatements[len(sc.vectorStatements)-1] = entry
			break
		}
	}
	if stmt == nil {
		if len(sc.vectorStatements) == maxShardVectorStatements {
			oldest := sc.vectorStatements[0]
			copy(sc.vectorStatements, sc.vectorStatements[1:])
			sc.vectorStatements[len(sc.vectorStatements)-1] = shardVectorStatement{}
			sc.vectorStatements = sc.vectorStatements[:len(sc.vectorStatements)-1]
			if err := oldest.stmt.Close(); err != nil {
				return nil, fmt.Errorf("close LRU vector statement: %w", err)
			}
		}
		var err error
		stmt, err = sc.db.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		sc.vectorStatements = append(sc.vectorStatements, shardVectorStatement{query: query, stmt: stmt})
	}
	rows, err := stmt.QueryContext(ctx, vector)
	if err != nil {
		// Do not retain a statement that failed before returning rows. The next
		// request can prepare afresh; no query is retried behind the caller.
		sc.vectorStatements[len(sc.vectorStatements)-1] = shardVectorStatement{}
		sc.vectorStatements = sc.vectorStatements[:len(sc.vectorStatements)-1]
		return nil, errors.Join(err, stmt.Close())
	}
	return rows, nil
}

// close is called under sc.mu after borrowers have closed their rows. Explicitly
// release statements before the native database (and its process-budget share).
func (sc *shardConn) close() error {
	var result error
	for _, entry := range sc.vectorStatements {
		result = errors.Join(result, entry.stmt.Close())
	}
	sc.vectorStatements = nil
	sc.metadataExpr = ""
	return errors.Join(result, sc.db.Close())
}

// Match the six-decimal literal values used by FormatVectorForSQL. Binding the
// raw float32s would otherwise subtly change distances and near-tie ranking.
func boundQueryVector(vector []float32) ([]float32, error) {
	bound := make([]float32, len(vector))
	for i, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, fmt.Errorf("query vector contains a non-finite value at index %d", i)
		}
		literal := strconv.FormatFloat(float64(value), 'f', 6, 32)
		rounded, err := strconv.ParseFloat(literal, 32)
		if err != nil {
			return nil, fmt.Errorf("round query vector at index %d: %w", i, err)
		}
		bound[i] = float32(rounded)
	}
	return bound, nil
}

func queryVectorRows(ctx context.Context, db docQueryer, vector []float32, k int, opts vectorQueryOpts, columns string) (*sql.Rows, error) {
	where, err := buildWhereClause(opts.filter, opts.documentIDs != nil)
	if err != nil {
		return nil, err
	}
	sc, cached := db.(*shardConn)
	cached = cached && opts.filter == nil && opts.documentIDs == nil
	value := "$1"
	if !cached {
		value = FormatVectorForSQL(vector)
	}
	distance := fmt.Sprintf("array_distance(embedding, %s::FLOAT[%d])", value, len(vector))
	order := distance
	if opts.documentIDs != nil {
		order = "(" + distance + " + 0.0)"
	}
	query := fmt.Sprintf("SELECT %s FROM docs%s ORDER BY %s LIMIT %d", fmt.Sprintf(columns, distance), where, order, k)
	if cached {
		bound, err := boundQueryVector(vector)
		if err != nil {
			return nil, err
		}
		return sc.queryPreparedVector(ctx, query, bound)
	}
	return db.QueryContext(ctx, query, documentScopeArgs(opts.documentIDs)...)
}
