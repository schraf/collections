package collections

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceArray_Basics(t *testing.T) {
	data := []uint32{10, 20, 30, 40, 50}
	a := NewSliceArray(data)

	assert.Equal(t, 5, a.Len())
	assert.Equal(t, uint32(10), a.At(0))
	assert.Equal(t, uint32(50), a.At(4))
}

func TestSliceArray_SliceZeroCopy(t *testing.T) {
	data := []uint32{1, 2, 3, 4, 5}
	a := NewSliceArray(data)

	view := a.Slice(1, 4, nil)
	assert.Equal(t, []uint32{2, 3, 4}, view)
	view[0] = 99
	assert.Equal(t, uint32(99), data[1], "nil-dest Slice should alias backing storage")
}

func TestSliceArray_SliceCopy(t *testing.T) {
	data := []uint32{1, 2, 3, 4, 5}
	a := NewSliceArray(data)

	dest := make([]uint32, 0, 8)
	got := a.Slice(1, 4, dest)
	assert.Equal(t, []uint32{2, 3, 4}, got)

	got[0] = 99
	assert.Equal(t, uint32(2), data[1], "non-nil-dest Slice should copy")
}

func TestSliceArray_SliceGrowsDest(t *testing.T) {
	data := []uint32{1, 2, 3, 4, 5}
	a := NewSliceArray(data)

	small := make([]uint32, 0, 1)
	got := a.Slice(0, 5, small)
	assert.Equal(t, data, got)
}
