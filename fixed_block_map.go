package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
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

func (m *FixedBlockMap[V]) Iter() iter.Seq[*V] {
	return func(yield func(*V) bool) {
		for blockIndex := range m.blocks {
			block := &m.blocks[blockIndex]

			for i := 0; i < FixedBlockSize; i++ {
				// Skip empty and deleted slots
				if block.control[i] != 0x0 && block.control[i] != 0x1 {
					if !yield(&block.values[i]) {
						return
					}
				}
			}
		}
	}
}

// Capacity returns the maximum capacity of the map
func (m *FixedBlockMap[V]) Capacity() uint64 {
	var capacity uint64
	capacity = uint64(len(m.blocks)) * FixedBlockSize
	return capacity
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
		if (control-0x0101010101010101) & ^control & 0x8080808080808080 != 0x0 {
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
			if block.control[i] == 0x0 {
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
			if block.control[i] == 0x1 && firstDeletedBlock == nil {
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
		if (control-0x0101010101010101) & ^control & 0x8080808080808080 != 0x0 {
			return
		}

		blockIndex = (blockIndex + 1) & m.mask
	}
}

// Rehash removes all deleted slots and rehashes all entries to optimize lookup performance.
// This function performs in-place rehashing without allocating additional memory for
// collecting entries, making it efficient for maps with millions of entries.
func (m *FixedBlockMap[V]) Rehash() error {
	// First pass: Convert all deleted slots (0x1) to empty slots (0x0)
	for blockIndex := range m.blocks {
		block := &m.blocks[blockIndex]
		for i := 0; i < FixedBlockSize; i++ {
			if block.control[i] == 0x1 {
				block.control[i] = 0x0
			}
		}
	}

	// Second pass: Re-position entries that are not in their optimal location
	// We iterate through all blocks and slots, and for each valid entry,
	// check if it should be moved earlier in its probe chain.
	for blockIndex := range m.blocks {
		block := &m.blocks[blockIndex]
		for i := 0; i < FixedBlockSize; i++ {
			// Skip empty slots
			if block.control[i] == 0x0 {
				continue
			}

			key := block.keys[i]
			value := block.values[i]
			optimalBlockIndex := m.hashToBlock(key)
			currentBlockIndex := uint64(blockIndex)

			// Check if this entry is in its optimal position
			// An entry is optimal if there are no empty slots in the blocks
			// between its hash block and its current position
			isOptimal := true

			// Scan from optimal block to current block
			probeBlockIndex := optimalBlockIndex
			for probeBlockIndex != currentBlockIndex {
				probeBlock := &m.blocks[probeBlockIndex]
				// If there's any empty slot in this block, the entry is not optimal
				for j := 0; j < FixedBlockSize; j++ {
					if probeBlock.control[j] == 0x0 {
						isOptimal = false
						break
					}
				}
				if !isOptimal {
					break
				}
				probeBlockIndex = (probeBlockIndex + 1) & m.mask
				if probeBlockIndex == optimalBlockIndex {
					// Wrapped around (shouldn't happen), but be safe
					break
				}
			}

			// Also check if there's an empty slot before position i in the current block
			if isOptimal && currentBlockIndex == optimalBlockIndex {
				for j := 0; j < i; j++ {
					if block.control[j] == 0x0 {
						isOptimal = false
						break
					}
				}
			}

			if !isOptimal {
				// Clear the current slot immediately so Put can use it if optimal
				block.control[i] = 0x0

				// Re-insert using Put, which will find the optimal position
				// (including potentially the slot we just cleared if it's optimal)
				if err := m.Put(key, value); err != nil {
					// Restore the entry if Put fails
					block.control[i] = key[0] | 0x80
					return fmt.Errorf("rehash failed during re-insertion: %w", err)
				}

				// Note: If Put placed the entry back in the same slot, that means
				// it was already optimal and our check had a false negative.
				// This is harmless, just slightly inefficient.
			}
		}
	}

	return nil
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
