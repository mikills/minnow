// duckdb_profile is a CI-only diagnostic probe, built against both engine versions.
// It is not a throughput benchmark or a replacement for the production simulations.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

type sample struct {
	Stage     string                   `json:"stage"`
	ElapsedNS int64                    `json:"elapsed_ns"`
	UserNS    int64                    `json:"user_ns"`
	SystemNS  int64                    `json:"system_ns"`
	GoHeap    uint64                   `json:"go_heap_bytes"`
	GoSys     uint64                   `json:"go_sys_bytes"`
	GoHeapSys uint64                   `json:"go_heap_sys_bytes"`
	GoRelease uint64                   `json:"go_heap_released_bytes"`
	GoStacks  uint64                   `json:"go_stack_in_use_bytes"`
	OS        map[string]uint64        `json:"os_kib"`
	Allocator map[string]uint64        `json:"glibc_bytes"`
	Jemalloc  *jemallocSample          `json:"jemalloc,omitempty"`
	DuckDB    []duckDBMemory           `json:"duckdb_memory,omitempty"`
	Mappings  map[string]mappingMemory `json:"mappings,omitempty"`
}

type probe struct {
	Mode              string            `json:"mode"`
	Version           string            `json:"version"`
	Source            string            `json:"source"`
	Iterations        int               `json:"iterations"`
	Samples           []sample          `json:"samples"`
	Plans             map[string]string `json:"query_plans,omitempty"`
	QueryPath         string            `json:"query_path,omitempty"`
	MemoryAttribution bool              `json:"memory_attribution,omitempty"`
	Settings          map[string]string `json:"settings,omitempty"`
	Diagnostics       []string          `json:"diagnostics,omitempty"`
	Parser            string            `json:"parser,omitempty"`
	ParserChecks      int               `json:"parser_checks,omitempty"`
	ParserSettings    map[string]string `json:"parser_settings,omitempty"`
	Error             string            `json:"error,omitempty"`
	memoryDB          *sql.DB
}

func allowed() bool {
	return os.Getenv("GITHUB_ACTIONS") == "true" && os.Getenv("RUNNER_ENVIRONMENT") == "github-hosted" && runtime.GOOS == "linux"
}

func main() {
	mode := flag.String("mode", "configured", "core, vss, fts, configured, ingest, query, or query_split")
	queryPath := flag.String("query-path", "all", "query_split path: all, adhoc, prepare, or reuse")
	extensions := flag.String("extensions", "", "absolute offline extension directory")
	expect := flag.String("expect", "", "required engine version")
	iterations := flag.Int("iterations", 20, "bounded operation count")
	memoryAttribution := flag.Bool("memory-attribution", false, "collect native allocator, DuckDB and mapping counters; purge arenas after close (diagnostic only)")
	parser := flag.String("parser", "", "CI-only 2.0 parser comparison: peg or legacy; loads signed legacy_parser in both arms")
	flag.Parse()
	if !allowed() {
		fmt.Fprintln(os.Stderr, "profiling is restricted to GitHub-hosted Linux Actions runners")
		os.Exit(1)
	}
	p := probe{Mode: *mode, Iterations: *iterations, QueryPath: *queryPath, MemoryAttribution: *memoryAttribution, Parser: *parser}
	err := p.run(*extensions, *expect)
	if err != nil {
		p.Error = err.Error()
	}
	if err := json.NewEncoder(os.Stdout).Encode(p); err != nil {
		panic(err)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (p *probe) timed(stage string, operation func() error) error {
	var before, after syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &before); err != nil {
		return err
	}
	start := time.Now()
	err := operation()
	elapsed := time.Since(start).Nanoseconds()
	if statErr := syscall.Getrusage(syscall.RUSAGE_SELF, &after); statErr != nil {
		return statErr
	}
	s := sample{Stage: stage, ElapsedNS: elapsed,
		UserNS: after.Utime.Nano() - before.Utime.Nano(), SystemNS: after.Stime.Nano() - before.Stime.Nano()}
	// Keep /proc and allocator inspection out of the hot query loop.
	if !hotQueryStage(stage) {
		if p.MemoryAttribution {
			if statErr := p.attributeMemory(&s); statErr != nil {
				return statErr
			}
		}
		if statErr := s.memory(); statErr != nil {
			return statErr
		}
	}
	p.Samples = append(p.Samples, s)
	return err
}

func (s *sample) memory() error {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	s.GoHeap = memory.HeapAlloc
	s.GoSys = memory.Sys
	s.GoHeapSys = memory.HeapSys
	s.GoRelease = memory.HeapReleased
	s.GoStacks = memory.StackInuse
	data, err := os.ReadFile("/proc/self/smaps_rollup")
	if err != nil {
		return err
	}
	s.OS = map[string]uint64{}
	for _, line := range strings.Split(string(data), "\n") {
		parts := strings.Fields(line)
		if len(parts) == 3 && parts[2] == "kB" {
			value, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				return err
			}
			s.OS[strings.TrimSuffix(parts[0], ":")] = value
		}
	}
	// malloc_info covers glibc's arenas, not Go's mmap heap or any alternative
	// native allocator. Preserve that distinction rather than calling RSS a leak.
	data = []byte(allocatorXML())
	var info struct {
		Totals []struct {
			Type string `xml:"type,attr"`
			Size uint64 `xml:"size,attr"`
		} `xml:"total"`
		System []struct {
			Type string `xml:"type,attr"`
			Size uint64 `xml:"size,attr"`
		} `xml:"system"`
	}
	if err := xml.Unmarshal(data, &info); err != nil {
		return fmt.Errorf("glibc malloc_info: %w", err)
	}
	s.Allocator = map[string]uint64{}
	for _, total := range info.Totals {
		s.Allocator[total.Type] = total.Size
	}
	for _, system := range info.System {
		s.Allocator["system_"+system.Type] = system.Size
	}
	return nil
}

func (p *probe) run(extensions, expect string) error {
	if extensions == "" || expect == "" || p.Iterations < 1 || p.Iterations > 10000 {
		return fmt.Errorf("extensions, expect, and 1..10000 iterations are required")
	}
	if err := validateParserControl(p.Parser, expect); err != nil {
		return err
	}
	switch p.Mode {
	case "core", "vss", "fts", "configured", "ingest", "query", "query_split":
	default:
		return fmt.Errorf("unknown mode %q", p.Mode)
	}
	if p.QueryPath != "all" && p.Mode != "query_split" {
		return fmt.Errorf("query-path is only valid with query_split")
	}
	if p.MemoryAttribution && (p.Mode == "query" || p.Mode == "query_split") {
		return fmt.Errorf("memory attribution is restricted to lifecycle/ingest probes")
	}
	switch p.QueryPath {
	case "all", "adhoc", "prepare", "reuse":
	default:
		return fmt.Errorf("unknown query-path %q", p.QueryPath)
	}
	dir, err := os.MkdirTemp("", "duckdb-profile-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := p.timed("initial", func() error { return nil }); err != nil {
		return err
	}
	count := p.Iterations
	if p.workloadDB() {
		count = 1
	}
	for i := 0; i < count; i++ {
		path := ""
		if p.workloadDB() {
			path = filepath.Join(dir, "data.duckdb")
		}
		if err := p.database(path, extensions, expect); err != nil {
			return err
		}
	}
	if err := p.timed("after_gc", func() error { runtime.GC(); return nil }); err != nil {
		return err
	}
	if p.MemoryAttribution {
		if err := p.timed("after_jemalloc_purge", func() error {
			if purgeErr := purgeJemalloc(); purgeErr != nil {
				p.Diagnostics = append(p.Diagnostics, purgeErr.Error())
			}
			return nil
		}); err != nil {
			return err
		}
	}
	// Diagnostic only: does returning glibc's free pages reduce residual RSS?
	// This is never applied to the production server or to the timed operations.
	return p.timed("after_trim", func() error { trimAllocator(); return nil })
}

func (p *probe) database(path, extensions, expect string) (err error) {
	var db *sql.DB
	err = p.timed("open", func() error {
		options := url.Values{
			"extension_directory": {extensions}, "autoinstall_known_extensions": {"false"},
			"autoload_known_extensions": {"false"}, "allow_unsigned_extensions": {"false"},
			"threads": {"1"}, "memory_limit": {"128MB"},
		}
		var openErr error
		db, openErr = sql.Open("duckdb", path+"?"+options.Encode())
		if openErr != nil {
			return openErr
		}
		db.SetMaxOpenConns(1)
		var codename string
		if err := db.QueryRow(`PRAGMA version`).Scan(&p.Version, &p.Source, &codename); err != nil {
			return err
		}
		if p.Version != expect {
			return fmt.Errorf("engine %s, expected %s", p.Version, expect)
		}
		for _, setting := range []string{"allow_unsigned_extensions", "autoinstall_known_extensions", "autoload_known_extensions"} {
			var enabled bool
			if err := db.QueryRow(`SELECT current_setting(?)`, setting).Scan(&enabled); err != nil {
				return err
			}
			if enabled {
				return fmt.Errorf("%s must stay disabled", setting)
			}
		}
		if p.MemoryAttribution {
			p.memoryDB = db
		}
		return nil
	})
	if db != nil {
		defer func() {
			if closeErr := p.timed("close", func() error {
				closeErr := db.Close()
				p.memoryDB = nil // Never reopen a closed DB just to inspect its counters.
				return closeErr
			}); err == nil {
				err = closeErr
			}
		}()
	}
	if err != nil {
		return err
	}
	if p.Parser != "" {
		// Match the loaded code and signature-verification work in both arms.
		// The initial open still uses PEG; this experiment cannot attribute all
		// startup grammar residency to the parser selected afterwards.
		if err := p.timed("load_legacy_parser", func() error {
			_, err := db.Exec("LOAD legacy_parser")
			return err
		}); err != nil {
			return err
		}
		if err := p.timed("select_parser", func() error {
			// Enabling legacy implicitly changes the override policy. Pin it in
			// both arms so only the enable flag differs and legacy cannot fall
			// back to PEG on syntax errors.
			if _, err := db.Exec("SET allow_parser_override_extension = 'strict'"); err != nil {
				return err
			}
			_, err := db.Exec("SET enable_legacy_parser = " + strconv.FormatBool(p.Parser == "legacy"))
			return err
		}); err != nil {
			return err
		}
		if err := p.checkParser(db); err != nil {
			return err
		}
		defer func() {
			if checkErr := p.checkParser(db); err == nil {
				err = checkErr
			}
		}()
	}
	queries := []struct{ stage, query string }{}
	if p.Mode == "vss" || p.Mode == "configured" || p.workloadDB() {
		queries = append(queries, struct{ stage, query string }{"load_vss", "LOAD vss"})
	}
	if p.Mode == "fts" || p.Mode == "configured" || p.workloadDB() {
		queries = append(queries, struct{ stage, query string }{"load_fts", "LOAD fts"})
	}
	for _, query := range queries {
		if err := p.timed(query.stage, func() error { _, err := db.Exec(query.query); return err }); err != nil {
			return err
		}
	}
	if !p.workloadDB() {
		return nil
	}
	if _, err := db.Exec(`SET hnsw_enable_experimental_persistence = true;
		CREATE TABLE docs (id TEXT, content TEXT, embedding FLOAT[32])`); err != nil {
		return err
	}
	batches := p.Iterations
	if p.Mode == "query" || p.Mode == "query_split" {
		batches = 8
	}
	for batch := 0; batch < batches; batch++ {
		if err := p.timed("append", func() error { return appendBatch(db, batch) }); err != nil {
			return err
		}
	}
	for _, query := range []struct{ stage, query string }{
		{"hnsw", `CREATE INDEX docs_vec_idx ON docs USING HNSW (embedding)`},
		{"fts_index", `PRAGMA create_fts_index('docs', 'id', 'content', stemmer='none', stopwords='none')`},
		{"checkpoint", "CHECKPOINT"},
	} {
		if err := p.timed(query.stage, func() error { _, err := db.Exec(query.query); return err }); err != nil {
			return err
		}
	}
	if p.Mode == "query_split" {
		return p.splitQueries(db)
	}
	if p.Mode == "query" {
		p.Plans = map[string]string{}
		for _, query := range profileQueries {
			rows, err := db.Query("EXPLAIN " + query.query)
			if err != nil {
				return err
			}
			var plan strings.Builder
			for rows.Next() {
				var key, value string
				if err := rows.Scan(&key, &value); err != nil {
					rows.Close()
					return err
				}
				plan.WriteString(value)
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			if err := rows.Close(); err != nil {
				return err
			}
			p.Plans[query.stage] = plan.String()
		}
		for i := 0; i < p.Iterations; i++ {
			for _, query := range profileQueries {
				if err := p.timed(query.stage, func() error {
					var id string
					return db.QueryRow(query.query).Scan(&id)
				}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateParserControl(parser, version string) error {
	if parser == "" {
		return nil
	}
	if (parser != "peg" && parser != "legacy") || version != "v2.0.0" {
		return fmt.Errorf("parser comparison requires peg or legacy and stable v2.0.0")
	}
	return nil
}

func (p *probe) checkParser(db *sql.DB) error {
	want := map[string]string{
		"allow_parser_override_extension": "strict",
		"enable_legacy_parser":            strconv.FormatBool(p.Parser == "legacy"),
		"allow_unsigned_extensions":       "false", "autoload_known_extensions": "false",
		"autoinstall_known_extensions": "false", "threads": "1", "memory_limit": "122.0 MiB",
	}
	// Keep the verification SQL order identical in both arms.
	for _, name := range []string{"enable_legacy_parser", "allow_parser_override_extension", "allow_unsigned_extensions", "autoload_known_extensions",
		"autoinstall_known_extensions", "threads", "memory_limit"} {
		expected := want[name]
		var got string
		if err := db.QueryRow("SELECT current_setting(?)::VARCHAR", name).Scan(&got); err != nil {
			return err
		}
		if got != expected {
			return fmt.Errorf("parser control %s: got %q, expected %q", name, got, expected)
		}
	}
	var loaded bool
	if err := db.QueryRow("SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'legacy_parser'").Scan(&loaded); err != nil {
		return err
	}
	if !loaded {
		return fmt.Errorf("legacy_parser must be loaded in both comparison arms")
	}
	want["legacy_parser_loaded"] = "true"
	p.ParserSettings = want
	p.ParserChecks++
	return nil
}

func (p *probe) workloadDB() bool {
	return p.Mode == "ingest" || p.Mode == "query" || p.Mode == "query_split"
}

func hotQueryStage(stage string) bool {
	if stage == "vector_query" || stage == "bm25_query" {
		return true
	}
	for _, prefix := range []string{"adhoc_", "prepare_", "fresh_execute_", "reuse_execute_", "stmt_close_"} {
		if strings.HasPrefix(stage, prefix) {
			return true
		}
	}
	return false
}

var profileQueries = []struct{ stage, query string }{
	{"vector_query", `SELECT id FROM docs ORDER BY array_distance(embedding, [1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]::FLOAT[32]) LIMIT 1`},
	{"bm25_query", `SELECT id FROM docs WHERE fts_main_docs.match_bm25(id, 'search') IS NOT NULL LIMIT 1`},
}

func appendBatch(db *sql.DB, batch int) error {
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Raw(func(raw any) error {
		appender, err := duckdb.NewAppenderFromConn(raw.(driver.Conn), "", "docs")
		if err != nil {
			return err
		}
		for i := 0; i < 512; i++ {
			vector := make([]float32, 32)
			vector[i%32] = 1
			if err := appender.AppendRow(fmt.Sprintf("doc-%d-%d", batch, i), "document search content", vector); err != nil {
				_ = appender.Close()
				return err
			}
		}
		return appender.Close()
	})
}
