package collections

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestNewStringArena(t *testing.T) {
	// Test with defaults
	arena := NewStringArena(0)
	assert.NotNil(t, arena)
	stats := arena.Stats()
	assert.Equal(t, 1, stats.NumChunks)
	assert.GreaterOrEqual(t, stats.TotalAllocated, 1024)
	assert.Equal(t, 0, stats.TotalUsed)

	// Test with specific values
	arena2 := NewStringArena(2000)
	assert.NotNil(t, arena2)
	stats2 := arena2.Stats()
	assert.Equal(t, 1, stats2.NumChunks)
	assert.Equal(t, 2000, stats2.TotalAllocated)
	assert.Equal(t, 0, stats2.TotalUsed)
}

func isPointerInChunks(ptr *byte, arena *StringArena) bool {
	pVal := uintptr(unsafe.Pointer(ptr))
	for _, chunk := range arena.chunks {
		if len(chunk) == 0 {
			continue
		}
		start := uintptr(unsafe.Pointer(&chunk[0]))
		end := start + uintptr(len(chunk))
		if pVal >= start && pVal < end {
			return true
		}
	}
	return false
}

func TestStringArena_AllocAndAllocBytes(t *testing.T) {
	arena := NewStringArena(1024)

	inputs := []string{
		"hello",
		"world",
		"go-collections",
		"performance",
	}

	allocated := make([]IndexedString, len(inputs))
	var err error

	for i, input := range inputs {
		if i%2 == 0 {
			allocated[i], err = arena.Alloc(input)
			assert.NoError(t, err)
		} else {
			allocated[i], err = arena.AllocBytes([]byte(input))
			assert.NoError(t, err)
		}

		got := arena.Get(allocated[i])

		// Verify content is identical
		assert.Equal(t, input, got)

		// Verify the memory is indeed inside the arena chunks
		strPtr := unsafe.StringData(got)
		assert.True(t, isPointerInChunks(strPtr, arena), "allocated string must be stored within the arena's memory")
	}

	// Verify all strings are still intact and didn't corrupt each other
	for i, input := range inputs {
		assert.Equal(t, input, arena.Get(allocated[i]))
	}

	stats := arena.Stats()
	assert.Equal(t, 1, stats.NumChunks)
	assert.Equal(t, 5+5+14+11, stats.TotalUsed)
}

func TestStringArena_Grow(t *testing.T) {
	arena := NewStringArena(1024) // 1024 bytes chunk size

	// Chunk size is 1024. Let's write strings that cross the 1024 boundary.
	idx1, err := arena.Alloc(string(make([]byte, 600)))
	assert.NoError(t, err)
	assert.Equal(t, 1, arena.Stats().NumChunks)

	// This allocation should exceed the remaining ~424 bytes in the first chunk, trigger growth,
	// and allocate a second chunk of 1024 bytes.
	idx2, err := arena.Alloc(string(make([]byte, 500)))
	assert.NoError(t, err)
	stats := arena.Stats()
	assert.Equal(t, 2, stats.NumChunks)

	str1 := arena.Get(idx1)
	str2 := arena.Get(idx2)

	// Check that both strings are backed by the arena and distinct
	assert.Len(t, str1, 600)
	assert.Len(t, str2, 500)
	assert.True(t, isPointerInChunks(unsafe.StringData(str1), arena))
	assert.True(t, isPointerInChunks(unsafe.StringData(str2), arena))
	assert.NotEqual(t, uintptr(unsafe.Pointer(unsafe.StringData(str1))), uintptr(unsafe.Pointer(unsafe.StringData(str2))))
}

func TestStringArena_VeryLargeString(t *testing.T) {
	arena := NewStringArena(1024) // 1024 bytes default chunk size

	// Allocate a string much larger than the default chunk size
	largeStrSize := 5000
	largeInput := string(make([]byte, largeStrSize))

	idx, err := arena.Alloc(largeInput)
	assert.NoError(t, err)
	got := arena.Get(idx)
	assert.Equal(t, largeStrSize, len(got))
	assert.True(t, isPointerInChunks(unsafe.StringData(got), arena))

	stats := arena.Stats()
	assert.Equal(t, 2, stats.NumChunks) // First default chunk (1024) + second customized chunk (5000)
	assert.GreaterOrEqual(t, stats.TotalAllocated, 6024)
}

func TestStringArena_ResetAndReuse(t *testing.T) {
	arena := NewStringArena(1024) // 1024 bytes chunk size

	// Phase 1: Allocate several strings causing a grow to 3 chunks
	_, _ = arena.Alloc(string(make([]byte, 800)))
	_, _ = arena.Alloc(string(make([]byte, 800)))
	_, _ = arena.Alloc(string(make([]byte, 800)))

	statsPhase1 := arena.Stats()
	assert.Equal(t, 3, statsPhase1.NumChunks)

	// Reset the arena
	arena.Reset()
	statsReset := arena.Stats()
	assert.Equal(t, 3, statsReset.NumChunks) // Chunks are kept
	assert.Equal(t, 0, statsReset.TotalUsed)

	// Phase 2: Re-allocate and verify we don't allocate new chunks unless needed
	idx1, err := arena.Alloc("hello")
	assert.NoError(t, err)
	idx2, err := arena.Alloc("world")
	assert.NoError(t, err)

	assert.Equal(t, "hello", arena.Get(idx1))
	assert.Equal(t, "world", arena.Get(idx2))

	statsPhase2 := arena.Stats()
	assert.Equal(t, 3, statsPhase2.NumChunks) // Still 3 chunks (reused first chunk)
	assert.Equal(t, 10, statsPhase2.TotalUsed)

	// Trigger a grow inside reuse
	// Allocate 900 bytes (fits in chunk 1, since chunkIndex=0, offset=10 => remaining is 1014)
	_, _ = arena.Alloc(string(make([]byte, 900)))

	// Now allocate another 900 bytes. This won't fit in chunk 1 (remaining ~114).
	// It should move to chunk 2 (which is 1024 bytes and satisfies 900) without creating a 4th chunk.
	_, _ = arena.Alloc(string(make([]byte, 900)))

	statsPhase3 := arena.Stats()
	assert.Equal(t, 3, statsPhase3.NumChunks) // Still reused chunk 2
}

func TestStringArena_ResetAndReuseWithResize(t *testing.T) {
	arena := NewStringArena(1024) // 1024 bytes chunk size

	// Allocate so we have a second chunk
	_, _ = arena.Alloc(string(make([]byte, 800)))
	_, _ = arena.Alloc(string(make([]byte, 800)))
	assert.Equal(t, 2, arena.Stats().NumChunks)

	arena.Reset()

	// Fill the first chunk
	_, _ = arena.Alloc(string(make([]byte, 800)))

	// Allocate a large string that exceeds the second chunk's size (1024)
	// This should replace the second chunk with a larger one
	_, _ = arena.Alloc(string(make([]byte, 2000)))

	stats := arena.Stats()
	assert.Equal(t, 2, stats.NumChunks)
	assert.GreaterOrEqual(t, stats.TotalAllocated, 3024)
}

func TestStringArena_EmptyInputs(t *testing.T) {
	arena := NewStringArena(1024)

	assert.Equal(t, "", arena.Get(EmptyIndexedString))
	assert.Equal(t, 0, arena.Stats().TotalUsed)
}
