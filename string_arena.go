package collections

import (
	"unsafe"
)

// StringArena is a fast, chunk-based memory allocator optimized for allocating strings.
// It pre-allocates contiguous memory blocks (chunks) and copies transient string/byte
// data into them. The returned strings refer directly to the arena's memory, avoiding
// standard heap allocations and reducing garbage collection (GC) pressure.
//
// StringArena is not safe for concurrent use by multiple goroutines.
type StringArena struct {
	chunks     [][]byte
	chunkIndex int
	chunkSize  int
	offset     int
}

// Stats holds memory and allocation statistics for a StringArena.
type Stats struct {
	TotalAllocated int // Total capacity of all allocated chunks in bytes
	TotalUsed      int // Total bytes actively used for strings in active chunks
	NumChunks      int // Total number of chunks allocated
}

// NewStringArena initializes a new StringArena with the given chunk size in bytes.
// It falls back to a sensible default if the provided size is non-positive.
func NewStringArena(chunkSize int) *StringArena {
	if chunkSize <= 0 {
		chunkSize = 16384 // 16KB default chunk size
	} else if chunkSize < 1024 {
		chunkSize = 1024 // enforce a minimum of 1KB per chunk
	}

	firstChunk := make([]byte, chunkSize)
	return &StringArena{
		chunks:     [][]byte{firstChunk},
		chunkIndex: 0,
		chunkSize:  chunkSize,
		offset:     0,
	}
}

// Alloc copies the content of s into the arena and returns a string pointing
// to the copied data. The returned string is backed by the arena's memory.
func (a *StringArena) Alloc(s string) string {
	if len(s) == 0 {
		return ""
	}

	if a.offset+len(s) > len(a.chunks[a.chunkIndex]) {
		a.grow(len(s))
	}

	chunk := a.chunks[a.chunkIndex]
	start := a.offset
	copy(chunk[start:], s)
	a.offset += len(s)

	// unsafe.String converts the byte slice pointer and length to a string
	// without allocating heap memory or copying bytes.
	return unsafe.String(&chunk[start], len(s))
}

// AllocBytes copies the content of b into the arena and returns a string pointing
// to the copied data. The returned string is backed by the arena's memory.
func (a *StringArena) AllocBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	if a.offset+len(b) > len(a.chunks[a.chunkIndex]) {
		a.grow(len(b))
	}

	chunk := a.chunks[a.chunkIndex]
	start := a.offset
	copy(chunk[start:], b)
	a.offset += len(b)

	// unsafe.String converts the byte slice pointer and length to a string
	// without allocating heap memory or copying bytes.
	return unsafe.String(&chunk[start], len(b))
}

// Reset resets the arena, allowing all of its chunks to be reused.
// This is a fast operation that does not allocate memory.
// Note: Resetting the arena invalidates all previously allocated strings.
func (a *StringArena) Reset() {
	a.chunkIndex = 0
	a.offset = 0
}

// Stats returns the current statistics of the arena.
func (a *StringArena) Stats() Stats {
	totalAllocated := 0
	totalUsed := 0

	for i := 0; i < len(a.chunks); i++ {
		totalAllocated += len(a.chunks[i])

		if i < a.chunkIndex {
			totalUsed += len(a.chunks[i])
		} else if i == a.chunkIndex {
			totalUsed += a.offset
		}
	}

	return Stats{
		TotalAllocated: totalAllocated,
		TotalUsed:      totalUsed,
		NumChunks:      len(a.chunks),
	}
}

func (a *StringArena) grow(required int) {
	size := a.chunkSize
	if required > size {
		size = required
	}

	// Check if there is an existing next chunk that can satisfy the requirement
	if a.chunkIndex+1 < len(a.chunks) {
		nextChunk := a.chunks[a.chunkIndex+1]
		if len(nextChunk) >= required {
			a.chunkIndex++
			a.offset = 0
			return
		}

		// The next chunk exists but is too small.
		// We replace it with a new, larger chunk.
		newChunk := make([]byte, size)
		a.chunks[a.chunkIndex+1] = newChunk
		a.chunkIndex++
		a.offset = 0
		return
	}

	// Allocate a new chunk and append it
	newChunk := make([]byte, size)
	a.chunks = append(a.chunks, newChunk)
	a.chunkIndex++
	a.offset = 0
}
