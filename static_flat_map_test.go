package collections

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticFlatMap_Basic(t *testing.T) {
	data := map[uint64]uint32{
		500_000_000: 13,
		100:         85,
		42:          33,
	}

	m := NewStaticFlatMap(data)

	value, found := m.Get(42)
	assert.True(t, found, "Key 42 should be found")
	assert.Equal(t, uint32(33), value)

	value, found = m.Get(500_000_000)
	assert.True(t, found, "Key 500,000,000 should be found")
	assert.Equal(t, uint32(13), value)

	value, found = m.Get(999)
	assert.False(t, found, "Key 999 should not exist in the sparse map")
	assert.Empty(t, value, "Returned value for missing key should be the zero value")
}
