//go:build linux && cgo

package main

/*
#define _GNU_SOURCE
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>

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
import "unsafe"

func allocatorXML() string {
	buffer := C.allocator_xml()
	if buffer == nil {
		return ""
	}
	defer C.free(unsafe.Pointer(buffer))
	return C.GoString(buffer)
}

func trimAllocator() { C.malloc_trim(0) }
