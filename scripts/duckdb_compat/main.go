// duckdb_compat checks the linked engine and persisted indexes across releases.
// CI builds this same source against both the base and candidate modules.
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	mode := flag.String("mode", "probe", "probe, write, or read")
	path := flag.String("file", "", "database file for write/read")
	extensions := flag.String("extensions", "", "absolute offline extension directory")
	expect := flag.String("expect", "", "expected engine version, if set")
	flag.Parse()
	if err := check(*mode, *path, *extensions, *expect); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func check(mode, path, extensions, expect string) (err error) {
	if mode != "probe" && mode != "write" && mode != "read" {
		return fmt.Errorf("unknown mode %q", mode)
	}
	if mode != "probe" && (path == "" || extensions == "") {
		return fmt.Errorf("write/read require -file and -extensions")
	}
	if mode == "write" {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return fmt.Errorf("write requires a new file: %s", path)
		}
	}
	if mode == "read" {
		if _, err := os.Stat(path); err != nil {
			return err
		}
		path += "?access_mode=read_only"
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	defer func() {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}()
	var version, source, codename string
	if err := db.QueryRow(`PRAGMA version`).Scan(&version, &source, &codename); err != nil {
		return err
	}
	if expect != "" && version != expect {
		return fmt.Errorf("engine %s, expected %s", version, expect)
	}
	if mode != "probe" {
		queries := []string{
			`SET extension_directory = '` + strings.ReplaceAll(extensions, "'", "''") + `'`,
			`SET allow_unsigned_extensions = false`,
			`SET autoinstall_known_extensions = false`,
			`SET autoload_known_extensions = false`,
			`LOAD vss`, `LOAD fts`, `LOAD httpfs`,
			`SET hnsw_enable_experimental_persistence = true`,
			`SET threads = 1`,
		}
		if mode == "write" {
			queries = append(queries,
				`CREATE TABLE docs (id TEXT, content TEXT, embedding FLOAT[8])`,
				`INSERT INTO docs VALUES ('legacy', 'legacy persisted search', [1,0,0,0,0,0,0,0]), ('other', 'unrelated document', [0,1,0,0,0,0,0,0])`,
				`CREATE INDEX docs_vec_idx ON docs USING HNSW (embedding)`,
				`PRAGMA create_fts_index('docs', 'id', 'content', stemmer='none', stopwords='none', ignore='', strip_accents=1, lower=1)`,
				`CHECKPOINT`)
		}
		for _, query := range queries {
			if _, err := db.Exec(query); err != nil {
				return fmt.Errorf("%s: %w", query, err)
			}
		}
		if mode == "read" {
			if err := checkIndexes(db); err != nil {
				return err
			}
		}
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]string{
		"version": version, "source": source, "mode": mode,
	})
}

func checkIndexes(db *sql.DB) error {
	vector := `SELECT id FROM docs ORDER BY array_distance(embedding, [1,0,0,0,0,0,0,0]::FLOAT[8]) LIMIT 1`
	for _, query := range []string{vector, `SELECT id FROM docs WHERE fts_main_docs.match_bm25(id, 'legacy') IS NOT NULL`} {
		var id string
		if err := db.QueryRow(query).Scan(&id); err != nil {
			return err
		}
		if id != "legacy" {
			return fmt.Errorf("index query returned %q, expected legacy", id)
		}
	}
	rows, err := db.Query(`EXPLAIN ` + vector)
	if err != nil {
		return err
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return err
		}
		plan.WriteString(value)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !strings.Contains(strings.ToUpper(strings.ReplaceAll(plan.String(), " ", "_")), "HNSW_INDEX_SCAN") {
		return fmt.Errorf("persisted index not used: %s", plan.String())
	}
	return nil
}
