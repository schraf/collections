package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"reflect"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

const FixedBlockSize = 8

type FixedBlockKey [16]byte

// FromString hashes the input text using xxHash.
// It populates 16 bytes using a 64-bit hash and a bitwise mixer.
func (k *FixedBlockKey) FromString(text string) {
	// Generate a 64-bit hash
	h := xxhash.Sum64([]byte(text))

	// Put the primary hash in the first 8 bytes
	// This will be used for the Block Index and Top Hash
	binary.LittleEndian.PutUint64(k[0:8], h)

	// Generate the second 8 bytes using a "mixer"
	// This ensures the full 16-byte key is unique even for different strings
	// that might have a 64-bit collision (though rare).
	// 0x9e3779b97f4a7c15 is the golden ratio constant used in many hashers
	h2 := h ^ (h >> 33)
	h2 *= 0x9e3779b97f4a7c15
	h2 ^= (h2 >> 33)

	binary.LittleEndian.PutUint64(k[8:16], h2)
}

type FixedBlock[V any] struct {
	control [FixedBlockSize]uint8
	keys    [FixedBlockSize]FixedBlockKey
	values  [FixedBlockSize]V
}

type FixedBlockMap[V any] struct {
	blocks []FixedBlock[V]
	mask   uint64
}

// NewFixedBlockMap initializes the map to support the given capacity
func NewFixedBlockMap[V any](capacity uint64) *FixedBlockMap[V] {
	// Calculate how many blocks we need using ceiling division
	blockCount := (capacity + FixedBlockSize - 1) / FixedBlockSize

	// Calculate the next power of two
	if blockCount <= 1 {
		blockCount = 1
	} else {
		blockCount = 1 << (64 - bits.LeadingZeros64(blockCount-1))
	}

	return &FixedBlockMap[V]{
		blocks: make([]FixedBlock[V], blockCount),
		mask:   blockCount - 1,
	}
}

// hashToBlock takes the 16-byte key (already a hash) and returns the starting block index.
func (m *FixedBlockMap[V]) hashToBlock(key FixedBlockKey) uint64 {
	// Use the first 8 bytes of the hash-key to pick the block
	return binary.LittleEndian.Uint64(key[:8]) & m.mask
}

// Get searches for a 16-byte key
func (m *FixedBlockMap[V]) Get(key FixedBlockKey) (*V, bool) {
	blockIndex := m.hashToBlock(key)
	tag := key[0] | 0x80 // MSB set + tag

	for {
		block := &m.blocks[blockIndex]
		control := binary.LittleEndian.Uint64(block.control[:])

		// Parallel search for the tag
		target := uint64(tag) * 0x0101010101010101
		match := control ^ target
		result := (match - 0x0101010101010101) & ^match & 0x8080808080808080

		for result != 0 {
			index := bits.TrailingZeros64(result) / 8
			if block.keys[index] == key {
				return &block.values[index], true
			}

			result &= result - 1 // Clear bit and keep looking in this block
		}

		// Check for an 'Empty' slot in this block to terminate search early
		// Logic: if any byte in control is 0x00, the search ends.
		if (control-0x0101010101010101) & ^control & 0x8080808080808080 != 0 {
			return nil, false
		}

		// Block full/no match. Probe to the next block
		blockIndex = (blockIndex + 1) & m.mask
	}
}

// Put inserts or updates a key
func (m *FixedBlockMap[V]) Put(key FixedBlockKey, value V) error {
	blockIndex := m.hashToBlock(key)
	tag := key[0] | 0x80

	var firstDeletedBlock *FixedBlock[V]
	var firstDeletedIndex int = -1

	for {
		block := &m.blocks[blockIndex]
		control := binary.LittleEndian.Uint64(block.control[:])

		// Check if key already exists (Update)
		target := uint64(tag) * 0x0101010101010101
		match := control ^ target
		result := (match - 0x0101010101010101) & ^match & 0x8080808080808080

		for result != 0 {
			index := bits.TrailingZeros64(result) / 8
			if block.keys[index] == key {
				block.values[index] = value
				return nil
			}

			result &= result - 1
		}

		// Look for an empty or deleted slot to insert
		for i := 0; i < 8; i++ {
			if block.control[i] == 0 {
				// If we found a tombstone earlier, use that instead to keep the chain short
				if firstDeletedBlock != nil {
					firstDeletedBlock.control[firstDeletedIndex] = tag
					firstDeletedBlock.keys[firstDeletedIndex] = key
					firstDeletedBlock.values[firstDeletedIndex] = value

					return nil
				}

				block.control[i] = tag
				block.keys[i] = key
				block.values[i] = value

				return nil
			}
			if block.control[i] == 0x01 && firstDeletedBlock == nil {
				firstDeletedBlock = block
				firstDeletedIndex = i
			}
		}

		// No space in this block. Check next block
		blockIndex = (blockIndex + 1) & m.mask

		// Safety check: if we looped back to start, the map is full
		if blockIndex == m.hashToBlock(key) {
			return errors.New("map overflow: no empty slots available")
		}
	}
}

// Delete marks a slot as deleted
func (m *FixedBlockMap[V]) Delete(key FixedBlockKey) {
	blockIndex := m.hashToBlock(key)
	tag := key[0] | 0x80

	for {
		block := &m.blocks[blockIndex]
		control := binary.LittleEndian.Uint64(block.control[:])

		target := uint64(tag) * 0x0101010101010101
		match := control ^ target
		result := (match - 0x0101010101010101) & ^match & 0x8080808080808080

		for result != 0 {
			index := bits.TrailingZeros64(result) / 8
			if block.keys[index] == key {
				block.control[index] = 0x1
				return
			}

			result &= result - 1
		}

		// If we hit an empty slot, the key isn't in the map
		if (control-0x0101010101010101) & ^control & 0x8080808080808080 != 0 {
			return
		}

		blockIndex = (blockIndex + 1) & m.mask
	}
}

// WriteTo writes the entire raw memory block of the map to an io.Writer.
func (m *FixedBlockMap[V]) WriteTo(w io.Writer) (int64, error) {
	if len(m.blocks) == 0 {
		return 0, nil
	}

	// Calculate the total size of the memory block
	blockSize := unsafe.Sizeof(m.blocks[0])
	totalSize := int(blockSize) * len(m.blocks)

	// Map the slice memory directly to a []byte for writing
	var blocks []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&blocks))
	header.Data = uintptr(unsafe.Pointer(&m.blocks[0]))
	header.Len = totalSize
	header.Cap = totalSize

	// Perform the write
	written, err := w.Write(blocks)
	return int64(written), err
}

// ReadFrom populates the map from an io.Reader.
// Note: The map must already be initialized with the correct capacity
// that matches the data being read.
func (m *FixedBlockMap[V]) ReadFrom(r io.Reader) (int64, error) {
	if len(m.blocks) == 0 {
		return 0, fmt.Errorf("map must be initialized with correct capacity before reading")
	}

	blockSize := unsafe.Sizeof(m.blocks[0])
	totalSize := int(blockSize) * len(m.blocks)

	// Map the slice memory directly to a []byte for reading
	var blocks []byte
	header := (*reflect.SliceHeader)(unsafe.Pointer(&blocks))
	header.Data = uintptr(unsafe.Pointer(&m.blocks[0]))
	header.Len = totalSize
	header.Cap = totalSize

	// Read directly into the buckets memory
	read, err := io.ReadFull(r, blocks)
	return int64(read), err
}
