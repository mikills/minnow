//go:build linux && cgo

package main

/*
#define _GNU_SOURCE
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>

// Use DuckDB's allocator, not libjemalloc from the runner or glibc. A weak
// reference reports missing instrumentation without replacing the shipped engine.
extern int duckdb_je_mallctl(const char *, void *, size_t *, void *, size_t) __attribute__((weak));

static int je_epoch(void) {
    if (!duckdb_je_mallctl) return ENOSYS;
    uint64_t epoch = 1;
    return duckdb_je_mallctl("epoch", NULL, NULL, &epoch, sizeof(epoch));
}

static int je_size(const char *name, size_t *value) {
    if (!duckdb_je_mallctl) return ENOSYS;
    size_t len = sizeof(*value);
    return duckdb_je_mallctl(name, value, &len, NULL, 0);
}

static const char *je_version(void) {
    if (!duckdb_je_mallctl) return NULL;
    const char *value = NULL;
    size_t len = sizeof(value);
    if (duckdb_je_mallctl("version", &value, &len, NULL, 0)) return NULL;
    return value;
}

static int je_purge(void) {
    if (!duckdb_je_mallctl) return ENOSYS;
    // MALLCTL_ARENAS_ALL is 4096 in the jemalloc 5.x public header bundled by
    // DuckDB. Purge arenas only: do not claim to flush other threads' tcaches.
    return duckdb_je_mallctl("arena.4096.purge", NULL, NULL, NULL, 0);
}

static char *allocator_xml(void) {
    char *buffer = NULL;
    size_t size = 0;
    FILE *stream = open_memstream(&buffer, &size);
    if (!stream) return NULL;
    int result = malloc_info(0, stream);
    fclose(stream);
    if (result != 0) { free(buffer); return NULL; }
    return buffer;
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func allocatorXML() string {
	buffer := C.allocator_xml()
	if buffer == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(buffer))
	return C.GoString(buffer)
}

func trimAllocator() { C.malloc_trim(0) }

func readJemalloc() *jemallocSample {
	result := &jemallocSample{Bytes: map[string]uint64{}}
	if version := C.je_version(); version != nil {
		result.Version = C.GoString(version)
	}
	if code := C.je_epoch(); code != 0 {
		result.Errors = append(result.Errors, fmt.Sprintf("jemalloc epoch unavailable: errno %d", code))
		return result
	}
	for _, key := range []string{"allocated", "active", "metadata", "resident", "mapped", "retained"} {
		name := C.CString("stats." + key)
		var value C.size_t
		code := C.je_size(name, &value)
		C.free(unsafe.Pointer(name))
		if code != 0 {
			result.Errors = append(result.Errors, fmt.Sprintf("jemalloc stats.%s unavailable: errno %d", key, code))
			continue
		}
		result.Bytes[key] = uint64(value)
	}
	return result
}

func purgeJemalloc() error {
	if code := C.je_purge(); code != 0 {
		return fmt.Errorf("jemalloc arena purge unavailable: errno %d", code)
	}
	return nil
}
