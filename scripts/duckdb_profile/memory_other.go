//go:build !linux || !cgo

package main

import "fmt"

// These stubs permit compile checks on developer machines. The executable's
// entry point refuses to run outside GitHub-hosted Linux runners.
func allocatorXML() string { return "" }
func trimAllocator()       {}

func readJemalloc() *jemallocSample {
	return &jemallocSample{Errors: []string{"jemalloc instrumentation requires Linux/cgo"}}
}

func purgeJemalloc() error { return fmt.Errorf("jemalloc purge requires Linux/cgo") }
