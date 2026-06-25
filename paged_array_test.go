package collections

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPagedArray_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.paged")

	data := make([]uint32, 1000)
	for i := range data {
		data[i] = uint32(i * 7)
	}

	// Small page size so the data spans many pages.
	require.NoError(t, CreatePagedArrayFile(path, data, 256))

	// Budget large enough for a handful of pages only.
	pa, err := OpenPagedArray[uint32](path, 4*256)
	require.NoError(t, err)
	defer pa.Close()

	assert.Equal(t, len(data), pa.Len())

	for i := range data {
		assert.Equal(t, data[i], pa.At(i), "At(%d)", i)
	}
}

func TestPagedArray_SliceSpansPages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.paged")

	data := make([]uint64, 500)
	for i := range data {
		data[i] = uint64(i)
	}

	// pageSize 256 / 8 bytes = 32 elems per page.
	require.NoError(t, CreatePagedArrayFile(path, data, 256))

	pa, err := OpenPagedArray[uint64](path, 2*256)
	require.NoError(t, err)
	defer pa.Close()

	// Range crossing several page boundaries.
	got := pa.Slice(10, 200, nil)
	require.Len(t, got, 190)
	for i := 0; i < 190; i++ {
		assert.Equal(t, uint64(10+i), got[i])
	}

	// Reuse a buffer; result must be stable and correct.
	buf := make([]uint64, 0)
	got2 := pa.Slice(100, 105, buf)
	assert.Equal(t, []uint64{100, 101, 102, 103, 104}, got2)
}

func TestPagedArray_ResidentCapNeverExceeded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.paged")

	data := make([]uint32, 2000)
	for i := range data {
		data[i] = uint32(i)
	}

	pageSize := 256 // 64 uint32 per page
	require.NoError(t, CreatePagedArrayFile(path, data, pageSize))

	// Budget of exactly 3 pages.
	maxPages := 3
	pa, err := OpenPagedArray[uint32](path, maxPages*pageSize)
	require.NoError(t, err)
	defer pa.Close()

	// Touch every element in a scattered order; resident pages must stay capped.
	for i := 0; i < len(data); i++ {
		idx := (i * 137) % len(data)
		assert.Equal(t, data[idx], pa.At(idx))
		assert.LessOrEqual(t, len(pa.pages), maxPages, "resident pages exceeded cap")
		assert.Equal(t, len(pa.pages), pa.lru.Len(), "pages map and lru list out of sync")
	}
}

func TestPagedArray_WritableScatterPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.paged")

	const n = 1000
	pageSize := 256 // 64 uint32 per page

	// Create a writable, pre-sized array and scatter values (reverse order to
	// force writes across many pages with eviction in between).
	w, err := createWritablePagedArray[uint32](path, n, pageSize, 2*pageSize)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, w.SetAt(i, uint32(i*3)))
	}
	require.NoError(t, w.Close())

	// Reopen read-only and verify all writes persisted through eviction/flush.
	ro, err := OpenPagedArray[uint32](path, 2*pageSize)
	require.NoError(t, err)
	defer ro.Close()

	assert.Equal(t, n, ro.Len())
	for i := 0; i < n; i++ {
		assert.Equal(t, uint32(i*3), ro.At(i), "At(%d)", i)
	}
}

func TestPagedArray_ShortFinalPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.paged")

	// 70 elems with 64 per page => last page has 6 elems.
	data := make([]uint32, 70)
	for i := range data {
		data[i] = uint32(1000 + i)
	}
	require.NoError(t, CreatePagedArrayFile(path, data, 256))

	pa, err := OpenPagedArray[uint32](path, 256)
	require.NoError(t, err)
	defer pa.Close()

	assert.Equal(t, uint32(1069), pa.At(69))
	got := pa.Slice(60, 70, nil)
	require.Len(t, got, 10)
	assert.Equal(t, uint32(1069), got[9])
}

func TestPagedArray_ElementSizeMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.paged")

	require.NoError(t, CreatePagedArrayFile(path, []uint32{1, 2, 3}, 256))

	// Opening with a differently-sized element type must fail.
	_, err := OpenPagedArray[uint64](path, 4096)
	assert.Error(t, err)
}
