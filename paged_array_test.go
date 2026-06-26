package collections

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagedArray_Basics(t *testing.T) {
	tempDir := t.TempDir()
	
	// Create PagedArray with a small maxLoadedPages
	pa, err := NewPagedArray[uint64](tempDir, 2)
	require.NoError(t, err)

	// Set some values
	err = pa.Set(0, 100)
	require.NoError(t, err)

	err = pa.Set(1, 200)
	require.NoError(t, err)

	err = pa.Set(1000, 300) // Different page assuming 4096 bytes / 8 = 512 elements per page
	require.NoError(t, err)

	// Get values
	val, err := pa.Get(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), val)

	val, err = pa.Get(1)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), val)

	val, err = pa.Get(1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(300), val)

	err = pa.Close()
	require.NoError(t, err)
}

func TestPagedArray_Eviction(t *testing.T) {
	tempDir := t.TempDir()
	
	// Cache holds 2 pages
	pa, err := NewPagedArray[uint64](tempDir, 2)
	require.NoError(t, err)

	elementsPerPage := uint64(PageSize) / 8 // 512

	// Write to 3 different pages, forcing eviction
	require.NoError(t, pa.Set(0, 10))
	require.NoError(t, pa.Set(elementsPerPage, 20))
	require.NoError(t, pa.Set(elementsPerPage*2, 30))

	// Get first element, which should have been evicted to disk
	val, err := pa.Get(0)
	require.NoError(t, err)
	assert.Equal(t, uint64(10), val)

	val, err = pa.Get(elementsPerPage)
	require.NoError(t, err)
	assert.Equal(t, uint64(20), val)

	val, err = pa.Get(elementsPerPage*2)
	require.NoError(t, err)
	assert.Equal(t, uint64(30), val)

	require.NoError(t, pa.Close())
}

func TestPagedArray_Persistence(t *testing.T) {
	tempDir := t.TempDir()
	
	pa, err := NewPagedArray[int32](tempDir, 5)
	require.NoError(t, err)

	require.NoError(t, pa.Set(50, 42))
	require.NoError(t, pa.Set(10000, 84))
	
	require.NoError(t, pa.Close())

	// Reopen
	pa2, err := NewPagedArray[int32](tempDir, 5)
	require.NoError(t, err)

	val, err := pa2.Get(50)
	require.NoError(t, err)
	assert.Equal(t, int32(42), val)

	val, err = pa2.Get(10000)
	require.NoError(t, err)
	assert.Equal(t, int32(84), val)

	// Unset values should be zero
	val, err = pa2.Get(51)
	require.NoError(t, err)
	assert.Equal(t, int32(0), val)

	require.NoError(t, pa2.Close())
}

func TestPagedArray_InvalidTypeSize(t *testing.T) {
	tempDir := t.TempDir()
	
	// Type size > 4096 (PageSize)
	type HugeStruct struct {
		data [5000]byte
	}

	pa, err := NewPagedArray[HugeStruct](tempDir, 2)
	assert.Error(t, err)
	assert.Nil(t, pa)
	assert.Contains(t, err.Error(), "exceeds maximum PageSize")
}

func TestPagedArray_MultipleSegments(t *testing.T) {
	tempDir := t.TempDir()
	pa, err := NewPagedArray[uint64](tempDir, 2)
	require.NoError(t, err)

	// Determine index that pushes us into the second segment
	// SegmentSize = 1 << 30 (1GB)
	// elementSize = 8 bytes
	// elementsPerSegment = SegmentSize / 8
	elementsPerSegment := uint64(SegmentSize) / 8

	// Write to segment 1
	idx1 := elementsPerSegment + 10
	require.NoError(t, pa.Set(idx1, 999))

	val, err := pa.Get(idx1)
	require.NoError(t, err)
	assert.Equal(t, uint64(999), val)

	require.NoError(t, pa.Close())

	// Check if the second segment file was created
	segmentFile := filepath.Join(tempDir, "segment_00001.bin")
	_, err = os.Stat(segmentFile)
	assert.NoError(t, err)
}
