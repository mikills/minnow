package centroid

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

func Compute(ctx context.Context, db *sql.DB) ([]float32, error) {
	rows, err := db.QueryContext(ctx, `SELECT CAST(embedding AS VARCHAR) FROM docs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sums, count, err := scanSums(rows)
	if err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if count == 0 || len(sums) == 0 {
		return nil, nil
	}
	return fromSums(sums, count), nil
}

func scanSums(rows *sql.Rows) ([]float64, int, error) {
	var sums []float64
	count := 0
	for rows.Next() {
		vec, err := scanVector(rows)
		if err != nil {
			return nil, 0, err
		}
		if len(vec) == 0 {
			continue
		}
		if err := addVector(&sums, vec); err != nil {
			return nil, 0, err
		}
		count++
	}
	return sums, count, nil
}

func scanVector(rows *sql.Rows) ([]float32, error) {
	var vecStr string
	if err := rows.Scan(&vecStr); err != nil {
		return nil, err
	}
	return ParseDuckDBVectorString(vecStr)
}

func addVector(sums *[]float64, vec []float32) error {
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

func fromSums(sums []float64, count int) []float32 {
	centroid := make([]float32, len(sums))
	for i := range sums {
		centroid[i] = float32(sums[i] / float64(count))
	}
	return centroid
}

func ParseDuckDBVectorString(raw string) ([]float32, error) {
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
