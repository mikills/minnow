package main

import (
	"strings"
	"testing"
)

// Pure parsing/control tests: no database, allocator purge or /proc inspection.
func TestMemory(t *testing.T) {
	t.Run("mappings", func(t *testing.T) {
		input := `1000-2000 r-xp 00000000 00:01 10 /a/vss.duckdb_extension
Size: 1000 kB
Rss: 10 kB
Anonymous: 2 kB
2000-3000 rw-p 00000001 00:01 10 /a/vss.duckdb_extension
Rss: 3 kB
Anonymous: 1 kB
3000-4000 rw-p 00000000 00:00 0
Rss: 7 kB
Anonymous: 7 kB
4000-5000 r-xp 00000000 00:01 20 /path with spaces/probe
Rss: 5 kB
Anonymous: 0 kB
VmFlags: rd ex mr mw me sd
`
		got, err := parseMappings(strings.NewReader(input))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got["/a/vss.duckdb_extension"] != (mappingMemory{RSS: 13, Anonymous: 3}) ||
			got["[anonymous]"].RSS != 7 || got["/path with spaces/probe"].RSS != 5 {
			t.Fatalf("incorrect resident mapping totals: %+v", got)
		}
	})
	t.Run("invalid_mappings", func(t *testing.T) {
		for _, input := range []string{"", "Rss: 1 kB\n", "zz-yy r-xp 0 00:01 1\nRss: 1 kB\n",
			"1000-2000 r-xp 0 00:01 1\nRss: -1 kB\n", "1000-2000 r-xp 0 00:01 1\nRss: 1 MB\n"} {
			if _, err := parseMappings(strings.NewReader(input)); err == nil {
				t.Fatalf("accepted invalid smaps: %q", input)
			}
		}
	})
}
