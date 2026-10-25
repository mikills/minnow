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
	Stage     string            `json:"stage"`
	ElapsedNS int64             `json:"elapsed_ns"`
	UserNS    int64             `json:"user_ns"`
	SystemNS  int64             `json:"system_ns"`
	GoHeap    uint64            `json:"go_heap_bytes"`
	GoSys     uint64            `json:"go_sys_bytes"`
	GoHeapSys uint64            `json:"go_heap_sys_bytes"`
	GoRelease uint64            `json:"go_heap_released_bytes"`
	GoStacks  uint64            `json:"go_stack_in_use_bytes"`
	OS        map[string]uint64 `json:"os_kib"`
	Allocator map[string]uint64 `json:"glibc_bytes"`
}

type probe struct {
	Mode       string            `json:"mode"`
	Version    string            `json:"version"`
	Source     string            `json:"source"`
	Iterations int               `json:"iterations"`
	Samples    []sample          `json:"samples"`
	Plans      map[string]string `json:"query_plans,omitempty"`
}

func allowed() bool {
	return os.Getenv("GITHUB_ACTIONS") == "true" && os.Getenv("RUNNER_ENVIRONMENT") == "github-hosted" && runtime.GOOS == "linux"
}

func main() {
	mode := flag.String("mode", "configured", "core, vss, fts, configured, ingest, or query")
	extensions := flag.String("extensions", "", "absolute offline extension directory")
	expect := flag.String("expect", "", "required engine version")
	iterations := flag.Int("iterations", 20, "bounded operation count")
	flag.Parse()
	if !allowed() {
		fmt.Fprintln(os.Stderr, "profiling is restricted to GitHub-hosted Linux Actions runners")
		os.Exit(1)
	}
	p := probe{Mode: *mode, Iterations: *iterations}
	if err := p.run(*extensions, *expect); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(p); err != nil {
		panic(err)
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
	if stage != "vector_query" && stage != "bm25_query" {
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
	switch p.Mode {
	case "core", "vss", "fts", "configured", "ingest", "query":
	default:
		return fmt.Errorf("unknown mode %q", p.Mode)
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
	if p.Mode == "ingest" || p.Mode == "query" {
		count = 1
	}
	for i := 0; i < count; i++ {
		path := ""
		if p.Mode == "ingest" || p.Mode == "query" {
			path = filepath.Join(dir, "data.duckdb")
		}
		if err := p.database(path, extensions, expect); err != nil {
			return err
		}
	}
	if err := p.timed("after_gc", func() error { runtime.GC(); return nil }); err != nil {
		return err
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
		return nil
	})
	if db != nil {
		defer func() {
			if closeErr := p.timed("close", db.Close); err == nil {
				err = closeErr
			}
		}()
	}
	if err != nil {
		return err
	}
	queries := []struct{ stage, query string }{}
	if p.Mode == "vss" || p.Mode == "configured" || p.Mode == "ingest" || p.Mode == "query" {
		queries = append(queries, struct{ stage, query string }{"load_vss", "LOAD vss"})
	}
	if p.Mode == "fts" || p.Mode == "configured" || p.Mode == "ingest" || p.Mode == "query" {
		queries = append(queries, struct{ stage, query string }{"load_fts", "LOAD fts"})
	}
	for _, query := range queries {
		if err := p.timed(query.stage, func() error { _, err := db.Exec(query.query); return err }); err != nil {
			return err
		}
	}
	if p.Mode != "ingest" && p.Mode != "query" {
		return nil
	}
	if _, err := db.Exec(`SET hnsw_enable_experimental_persistence = true;
		CREATE TABLE docs (id TEXT, content TEXT, embedding FLOAT[32])`); err != nil {
		return err
	}
	batches := p.Iterations
	if p.Mode == "query" {
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
