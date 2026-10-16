//go:build duckdb_use_static_lib

package connection

/*
#include <duckdb_static_extension.h>

// DuckDB 2.0 static archives require explicit extension/capability registration
// before the first database opens. See DuckDB's static_extension_loader target.
#define REGISTER(name) \
    extern int32_t duckdb_extension_##name##_describe(duckdb_extension_descriptor *); \
    if (duckdb_register_static_extension(duckdb_extension_##name##_describe) != 0) return 1;

static int minnow_register_duckdb_extensions(void) {
    REGISTER(core_functions)
    REGISTER(icu)
    REGISTER(json)
    REGISTER(parquet)
    REGISTER(autocomplete)
    REGISTER(httplib)
    REGISTER(loadable_extensions)
    return 0;
}
*/
import "C"

func init() {
	if C.minnow_register_duckdb_extensions() != 0 {
		panic("register DuckDB static extensions and capabilities")
	}
}
