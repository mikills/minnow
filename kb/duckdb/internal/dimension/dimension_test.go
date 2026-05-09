package dimension

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDuckDBArray(t *testing.T) {
	got, err := ParseDuckDBArray("FLOAT[384]")
	require.NoError(t, err)
	require.Equal(t, 384, got)

	_, err = ParseDuckDBArray("FLOAT[]")
	require.Error(t, err)
}

func TestValidateVector(t *testing.T) {
	require.NoError(t, ValidateVector([]float32{1, 2}, 2, "query"))
	require.Error(t, ValidateVector([]float32{1}, 2, "query"))
}
