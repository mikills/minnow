package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type splitQuery struct {
	name string
	sql  string
	// A parameterized vector changes between two directions on successive
	// iterations. Fixed SQL is an upper bound on what statement reuse can save.
	parameterized bool
	vector        bool
}

func splitCases() []splitQuery {
	return []splitQuery{
		{name: "vector", sql: profileQueries[0].query, vector: true},
		{name: "bm25", sql: profileQueries[1].query},
		{name: "bound_vector", vector: true, parameterized: true,
			sql: `SELECT id FROM docs ORDER BY array_distance(embedding, ?::FLOAT[32]) LIMIT 1`},
	}
}

func (q splitQuery) args(iteration int) []any {
	if !q.parameterized {
		return nil
	}
	vector := make([]float32, 32)
	vector[iteration%2] = 1
	return []any{vector}
}

func (q splitQuery) checkID(id string, iteration int) error {
	var batch, row int
	if _, err := fmt.Sscanf(id, "doc-%d-%d", &batch, &row); err != nil || batch < 0 || batch >= 8 || row < 0 || row >= 512 || id != fmt.Sprintf("doc-%d-%d", batch, row) {
		return fmt.Errorf("%s returned invalid fixture ID %q", q.name, id)
	}
	axis := 0
	if q.parameterized {
		axis = iteration % 2
	}
	if q.vector && row%32 != axis {
		return fmt.Errorf("%s returned %q for axis %d; stale parameters or wrong result", q.name, id, axis)
	}
	return nil
}

func explainSplit(ctx context.Context, conn *sql.Conn, q splitQuery, iteration int) (string, error) {
	rows, err := conn.QueryContext(ctx, "EXPLAIN "+q.sql, q.args(iteration)...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return "", err
		}
		plan.WriteString(value)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if q.vector && !q.parameterized && !strings.Contains(strings.ToUpper(strings.ReplaceAll(plan.String(), " ", "_")), "HNSW_INDEX_SCAN") {
		return "", fmt.Errorf("%s lost HNSW in EXPLAIN: %s", q.name, plan.String())
	}
	return plan.String(), nil
}

func (p *probe) splitQueries(db *sql.DB) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	p.Plans = map[string]string{}
	for _, q := range splitCases() {
		plan, err := explainSplit(ctx, conn, q, 0)
		if err != nil {
			return err
		}
		p.Plans[q.name] = plan
		if q.parameterized {
			plan, err := explainSplit(ctx, conn, q, 1)
			if err != nil {
				return err
			}
			p.Plans[q.name+"_axis1"] = plan
		}
		if err := p.splitQuery(ctx, conn, q); err != nil {
			return err
		}
	}
	return nil
}

func (p *probe) splitQuery(ctx context.Context, conn *sql.Conn, q splitQuery) error {
	var reused *sql.Stmt
	if p.QueryPath == "all" || p.QueryPath == "reuse" {
		var err error
		reused, err = conn.PrepareContext(ctx, q.sql)
		if err != nil {
			return err
		}
		defer reused.Close()
	}
	// Warm both paths with the same inputs; warmups are not measured. All paths
	// scan the same single ID, and validate results outside the operation timer.
	for i := 0; i < 10; i++ {
		var id string
		if err := conn.QueryRowContext(ctx, q.sql, q.args(i)...).Scan(&id); err != nil {
			return err
		}
		if err := q.checkID(id, i); err != nil {
			return err
		}
		if reused != nil {
			if err := reused.QueryRowContext(ctx, q.args(i)...).Scan(&id); err != nil {
				return err
			}
			if err := q.checkID(id, i); err != nil {
				return err
			}
		}
	}
	for i := 0; i < p.Iterations; i++ {
		args := q.args(i)
		selectID := func(stage string, query func() *sql.Row) error {
			var id string
			if err := p.timed(stage+"_"+q.name, func() error { return query().Scan(&id) }); err != nil {
				return err
			}
			return q.checkID(id, i)
		}
		adhoc := func() error {
			if p.QueryPath != "all" && p.QueryPath != "adhoc" {
				return nil
			}
			return selectID("adhoc", func() *sql.Row { return conn.QueryRowContext(ctx, q.sql, args...) })
		}
		cached := func() error {
			if reused == nil {
				return nil
			}
			return selectID("reuse_execute", func() *sql.Row { return reused.QueryRowContext(ctx, args...) })
		}
		paths := []func() error{adhoc, cached}
		if i%2 == 1 {
			paths[0], paths[1] = paths[1], paths[0]
		}
		for _, path := range paths {
			if err := path(); err != nil {
				return err
			}
		}
		if p.QueryPath == "all" || p.QueryPath == "prepare" {
			var fresh *sql.Stmt
			if err := p.timed("prepare_"+q.name, func() error {
				var err error
				fresh, err = conn.PrepareContext(ctx, q.sql)
				return err
			}); err != nil {
				return err
			}
			if p.QueryPath == "all" {
				if err := selectID("fresh_execute", func() *sql.Row { return fresh.QueryRowContext(ctx, args...) }); err != nil {
					fresh.Close()
					return err
				}
			}
			if err := p.timed("stmt_close_"+q.name, fresh.Close); err != nil {
				return err
			}
		}
	}
	return nil
}
