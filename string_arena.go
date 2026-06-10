package collections

import (
	"errors"
	"math"
	"unsafe"
)

var EmptyIndexedString = IndexedString(math.MaxUint64)

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

// IndexedString holds a reference to a string inside of a StringArena.
type IndexedString uint64

func newIndexedString(chunk uint8, offset uint32, length uint32) IndexedString {
	return IndexedString((uint64(chunk) << 56) | (uint64(offset) << 24) | (uint64(length) & 0xFFFFFF))
}

func (i IndexedString) chunk() uint8 {
	return uint8(uint64(i) >> 56)
}

func (i IndexedString) offset() uint32 {
	return uint32((uint64(i) >> 24) & 0xFFFFFFFF)
}

func (i IndexedString) length() uint32 {
	return uint32(uint64(i) & 0xFFFFFF)
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

// Alloc copies the content of s into the arena and returns an index to the copied data.
func (a *StringArena) Alloc(s string) (IndexedString, error) {
	if len(s) == 0 {
		return EmptyIndexedString, nil
	}

	if a.offset+len(s) > len(a.chunks[a.chunkIndex]) {
		if a.chunkIndex == math.MaxUint8 {
			return EmptyIndexedString, errors.New("string arena memory overflow")
		}

		a.grow(len(s))
	}

	chunk := a.chunks[a.chunkIndex]
	start := a.offset
	copy(chunk[start:], s)
	a.offset += len(s)

	return newIndexedString(uint8(a.chunkIndex), uint32(start), uint32(len(s))), nil
}

// AllocBytes copies the content of b into the arena and returns an index to the copied data.
func (a *StringArena) AllocBytes(b []byte) (IndexedString, error) {
	if len(b) == 0 {
		return EmptyIndexedString, nil
	}

	if a.offset+len(b) > len(a.chunks[a.chunkIndex]) {
		if a.chunkIndex == math.MaxUint8 {
			return EmptyIndexedString, errors.New("string arena memory overflow")
		}

		a.grow(len(b))
	}

	chunk := a.chunks[a.chunkIndex]
	start := a.offset
	copy(chunk[start:], b)
	a.offset += len(b)

	return newIndexedString(uint8(a.chunkIndex), uint32(start), uint32(len(b))), nil
}

// Get returns the string for the given index.
func (a *StringArena) Get(i IndexedString) string {
	if i == EmptyIndexedString {
		return ""
	}

	chunkIdx := int(i.chunk())
	if chunkIdx < 0 || chunkIdx > a.chunkIndex || chunkIdx >= len(a.chunks) {
		return ""
	}

	chunk := a.chunks[chunkIdx]
	start := int(i.offset())
	length := int(i.length())
	if start < 0 || length < 0 || start+length > len(chunk) {
		return ""
	}

	return unsafe.String(&chunk[start], length)
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
