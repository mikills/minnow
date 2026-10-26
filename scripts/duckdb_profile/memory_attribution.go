package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type jemallocSample struct {
	Version string            `json:"version,omitempty"`
	Bytes   map[string]uint64 `json:"bytes"`
	Errors  []string          `json:"errors,omitempty"`
}

type duckDBMemory struct {
	Tag       string `json:"tag"`
	Memory    int64  `json:"memory_usage_bytes"`
	Temporary int64  `json:"temporary_storage_bytes"`
}

type mappingMemory struct {
	RSS       uint64 `json:"rss_kib"`
	Anonymous uint64 `json:"anonymous_kib"`
}

func (p *probe) attributeMemory(s *sample) error {
	// This SQL is deliberately outside the operation timer. Its own allocations
	// can perturb snapshots, so these runs are not latency benchmarks. Close rows
	// before reading allocator/RSS counters. No SQL is run after database close.
	if p.memoryDB != nil {
		rows, err := p.memoryDB.Query(`SELECT tag, memory_usage_bytes, temporary_storage_bytes FROM duckdb_memory()`)
		if err != nil {
			return fmt.Errorf("duckdb_memory: %w", err)
		}
		for rows.Next() {
			var entry duckDBMemory
			if err := rows.Scan(&entry.Tag, &entry.Memory, &entry.Temporary); err != nil {
				_ = rows.Close()
				return err
			}
			s.DuckDB = append(s.DuckDB, entry)
		}
		rowsErr := rows.Err()
		closeErr := rows.Close()
		if rowsErr != nil {
			return rowsErr
		}
		if closeErr != nil {
			return closeErr
		}
		if p.Settings == nil {
			p.Settings = map[string]string{}
			for _, name := range []string{"memory_limit", "threads", "allocator_background_threads", "allocator_flush_threshold", "allocator_bulk_deallocation_flush_threshold"} {
				var value string
				if err := p.memoryDB.QueryRow(`SELECT current_setting(?)::VARCHAR`, name).Scan(&value); err != nil {
					return fmt.Errorf("read %s: %w", name, err)
				}
				p.Settings[name] = value
			}
		}
	}
	s.Jemalloc = readJemalloc()
	file, err := os.Open("/proc/self/smaps")
	if err != nil {
		return err
	}
	defer file.Close()
	s.Mappings, err = parseMappings(file)
	return err
}

// Aggregate VMAs by their actual path (not file size or virtual address range).
// Anonymous/special mappings remain visible; RSS and anonymous bytes overlap.
func parseMappings(reader io.Reader) (map[string]mappingMemory, error) {
	result := map[string]mappingMemory{}
	name := ""
	haveHeader := false
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && strings.Contains(fields[0], "-") {
			addresses := strings.Split(fields[0], "-")
			if len(addresses) != 2 {
				return nil, fmt.Errorf("invalid smaps header")
			}
			for _, address := range addresses {
				if _, err := strconv.ParseUint(address, 16, 64); err != nil {
					return nil, fmt.Errorf("invalid smaps address: %w", err)
				}
			}
			name = "[anonymous]"
			if len(fields) > 5 {
				name = strings.Join(fields[5:], " ")
			}
			haveHeader = true
			continue
		}
		if len(fields) == 0 || (fields[0] != "Rss:" && fields[0] != "Anonymous:") {
			continue
		}
		if !haveHeader || len(fields) != 3 || fields[2] != "kB" {
			return nil, fmt.Errorf("invalid smaps memory row")
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return nil, err
		}
		entry := result[name]
		if fields[0] == "Rss:" {
			entry.RSS += value
		} else {
			entry.Anonymous += value
		}
		result[name] = entry
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty smaps counters")
	}
	return result, nil
}
