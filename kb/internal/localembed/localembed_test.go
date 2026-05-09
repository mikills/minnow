package localembed

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVector(t *testing.T) {
	vec, err := Vector("ferret habitat", 8, 3, 6)
	require.NoError(t, err)
	require.Len(t, vec, 8)
	require.NotEqual(t, make([]float32, 8), vec)
}

func TestTokenize(t *testing.T) {
	require.Equal(t, []string{"hello", "world", "42"}, Tokenize(NormalizeInput(" Hello, WORLD 42 ")))
}
