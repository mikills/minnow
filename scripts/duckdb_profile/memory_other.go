//go:build !linux || !cgo

package main

// These stubs permit compile checks on developer machines. The executable's
// entry point refuses to run outside GitHub-hosted Linux runners.
func allocatorXML() string { return "" }
func trimAllocator()       {}
