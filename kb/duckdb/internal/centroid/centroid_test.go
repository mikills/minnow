package centroid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDuckDBVectorString(t *testing.T) {
	got, err := ParseDuckDBVectorString("[1, 2.5, 3]")
	require.NoError(t, err)
	require.Equal(t, []float32{1, 2.5, 3}, got)

	got, err = ParseDuckDBVectorString("[]")
	require.NoError(t, err)
	require.Empty(t, got)

	_, err = ParseDuckDBVectorString("[nope]")
	require.Error(t, err)
}
