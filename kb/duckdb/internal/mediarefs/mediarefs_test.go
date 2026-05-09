package mediarefs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   []string
	}{
		{"empty", nil},
		{"single", []string{"m1"}},
		{"multi", []string{"m1", "m2"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := Encode(tc.in, nil)
			require.NoError(t, err)
			decoded, err := Decode(encoded)
			require.NoError(t, err)
			if len(tc.in) == 0 {
				require.Nil(t, decoded)
				return
			}
			require.Len(t, decoded, len(tc.in))
			for i, id := range tc.in {
				require.Equal(t, id, decoded[i].MediaID)
			}
		})
	}
}
