package main

import (
	"bufio"
	"database/sql"
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
	RSS           uint64 `json:"rss_kib"`
	Anonymous     uint64 `json:"anonymous_kib"`
	AnonHugePages uint64 `json:"anon_huge_pages_kib"`
}

func validateMemoryQueryID(stage, id string, batches int) error {
	parts := strings.Split(id, "-")
	if len(parts) != 3 || parts[0] != "doc" || batches <= 0 {
		return fmt.Errorf("%s returned invalid fixture ID %q", stage, id)
	}
	batch, batchErr := strconv.Atoi(parts[1])
	row, rowErr := strconv.Atoi(parts[2])
	if batchErr != nil || rowErr != nil || batch < 0 || batch >= batches || row < 0 || row >= 512 ||
		id != fmt.Sprintf("doc-%d-%d", batch, row) {
		return fmt.Errorf("%s returned out-of-range or noncanonical fixture ID %q", stage, id)
	}
	if stage == "vector_query" && row%32 != 0 {
		return fmt.Errorf("vector query returned wrong-axis fixture ID %q", id)
	}
	if stage != "vector_query" && stage != "bm25_query" {
		return fmt.Errorf("unknown fixture query %q", stage)
	}
	return nil
}

func (p *probe) validateTHPControl() error {
	if p.THPPolicy == "" {
		return nil
	}
	if (p.THPPolicy != "default" && p.THPPolicy != "disabled") || !p.MemoryAttribution || p.Parser != "" {
		return fmt.Errorf("THP comparison requires default or disabled, memory attribution, and no parser override")
	}
	return nil
}

func (p *probe) checkTHPSettings(db *sql.DB) error {
	// Check before loading extensions and again before closing each database.
	for _, setting := range []struct{ name, want string }{
		{"allow_unsigned_extensions", "false"}, {"autoinstall_known_extensions", "false"},
		{"autoload_known_extensions", "false"}, {"threads", "1"}, {"memory_limit", "122.0 MiB"},
	} {
		var got string
		if err := db.QueryRow(`SELECT current_setting(?)::VARCHAR`, setting.name).Scan(&got); err != nil {
			return err
		}
		if got != setting.want {
			return fmt.Errorf("THP control %s: got %q, expected %q", setting.name, got, setting.want)
		}
	}
	p.THPChecks++
	return nil
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
	seen := uint8(0)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && strings.Contains(fields[0], "-") {
			if haveHeader && seen != 7 {
				return nil, fmt.Errorf("incomplete smaps memory counters for %s", name)
			}
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
			seen = 0
			continue
		}
		if len(fields) == 0 || (fields[0] != "Rss:" && fields[0] != "Anonymous:" && fields[0] != "AnonHugePages:") {
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
		var bit uint8
		if fields[0] == "Rss:" {
			bit = 1
			entry.RSS += value
		} else if fields[0] == "Anonymous:" {
			bit = 2
			entry.Anonymous += value
		} else {
			bit = 4
			entry.AnonHugePages += value
		}
		if seen&bit != 0 {
			return nil, fmt.Errorf("duplicate smaps counter %s", fields[0])
		}
		seen |= bit
		result[name] = entry
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if haveHeader && seen != 7 {
		return nil, fmt.Errorf("incomplete smaps memory counters for %s", name)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty smaps counters")
	}
	return result, nil
}
