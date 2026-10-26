package main

import "testing"

// Pure flag checks: the database experiment runs only on hosted Linux CI.
func TestParser(t *testing.T) {
	t.Run("controls", func(t *testing.T) {
		for _, tc := range []struct {
			parser, version string
			valid           bool
		}{
			{"", "v1.5.0", true}, {"", "v2.0.0", true},
			{"peg", "v2.0.0", true}, {"legacy", "v2.0.0", true},
			{"legacy", "v1.5.0", false}, {"peg", "v2.0.0-dev", false},
			{"unknown", "v2.0.0", false},
		} {
			if err := validateParserControl(tc.parser, tc.version); (err == nil) != tc.valid {
				t.Fatalf("parser=%q version=%q: %v", tc.parser, tc.version, err)
			}
		}
	})
}
