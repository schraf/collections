package collections

import (
	"container/list"
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

type FixedBlockMapInfo struct {
	// ratio of stored entities to capacity
	LoadFactor float32

	// ratio of tombstones to total slots
	TombstoneFactor float32

	// set to true when TombstoreFactor value indicates that it would
	// be beneficial to rehash the map
	RecommendRehash bool

	// set to true when LoadFactor value indicates that it would
	// be beneficial to grow the map
	RecommendGrow bool
}

type FixedBlock[V any] struct {
	control uint64 // 8 control bytes packed into a single uint64 for fast SIMD-like operations
	keys    [FixedBlockSize]FixedBlockKey
	values  [FixedBlockSize]V
}

// controlByte extracts a single control byte at the given index (0-7)
func (b *FixedBlock[V]) controlByte(index int) uint8 {
	return uint8(b.control >> (index * 8))
}

// setControlByte sets a single control byte at the given index (0-7)
func (b *FixedBlock[V]) setControlByte(index int, value uint8) {
	shift := index * 8
	b.control = (b.control &^ (0xFF << shift)) | (uint64(value) << shift)
}

type FixedBlockMap[V any] struct {
	blocks []FixedBlock[V]
	mask   uint64
}

// calculateBlockCount calculates the number of blocks needed for a given capacity.
// Returns the next power of two that can accommodate the capacity.
func calculateBlockCount(capacity uint64) uint64 {
	// Calculate how many blocks we need using ceiling division
	blockCount := (capacity + FixedBlockSize - 1) / FixedBlockSize

	// Calculate the next power of two
	if blockCount <= 1 {
		blockCount = 1
	} else {
		blockCount = 1 << (64 - bits.LeadingZeros64(blockCount-1))
	}

	return blockCount
}

// NewFixedBlockMap initializes the map to support the given capacity
func NewFixedBlockMap[V any](capacity uint64) *FixedBlockMap[V] {
	blockCount := calculateBlockCount(capacity)

	return &FixedBlockMap[V]{
		blocks: make([]FixedBlock[V], blockCount),
		mask:   blockCount - 1,
	}
}

func (m *FixedBlockMap[V]) Iter() iter.Seq2[FixedBlockKey, *V] {
	return func(yield func(FixedBlockKey, *V) bool) {
		for blockIndex := range m.blocks {
			block := &m.blocks[blockIndex]

			for i := 0; i < FixedBlockSize; i++ {
				// Skip empty and deleted slots
				ctrl := block.controlByte(i)
				if ctrl != 0x0 && ctrl != 0x1 {
					if !yield(block.keys[i], &block.values[i]) {
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
	// Direct memory read - assumes little-endian architecture
	return *(*uint64)(unsafe.Pointer(&key[0])) & m.mask
}

// Get searches for a 16-byte key
func (m *FixedBlockMap[V]) Get(key FixedBlockKey) (*V, bool) {
	blockIndex := m.hashToBlock(key)
	tag := key[0] | 0x80 // MSB set + tag

	for {
		block := &m.blocks[blockIndex]
		control := block.control

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
		control := block.control

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
			ctrl := block.controlByte(i)
			if ctrl == 0x0 {
				// If we found a tombstone earlier, use that instead to keep the chain short
				if firstDeletedBlock != nil {
					firstDeletedBlock.setControlByte(firstDeletedIndex, tag)
					firstDeletedBlock.keys[firstDeletedIndex] = key
					firstDeletedBlock.values[firstDeletedIndex] = value

					return nil
				}

				block.setControlByte(i, tag)
				block.keys[i] = key
				block.values[i] = value

				return nil
			}
			if ctrl == 0x1 && firstDeletedBlock == nil {
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
		control := block.control

		target := uint64(tag) * 0x0101010101010101
		match := control ^ target
		result := (match - 0x0101010101010101) & ^match & 0x8080808080808080

		for result != 0 {
			index := bits.TrailingZeros64(result) / 8
			if block.keys[index] == key {
				block.setControlByte(index, 0x1)
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

func (m *FixedBlockMap[V]) CollectInfo() FixedBlockMapInfo {
	var storedEntities uint64
	var tombstones uint64
	totalSlots := m.Capacity()

	// Count stored entities and tombstones
	for blockIndex := range m.blocks {
		block := &m.blocks[blockIndex]
		for i := 0; i < FixedBlockSize; i++ {
			ctrl := block.controlByte(i)
			if ctrl == 0x1 {
				// Tombstone (deleted slot)
				tombstones++
			} else if ctrl != 0x0 {
				// Stored entity (non-empty, non-deleted)
				storedEntities++
			}
		}
	}

	// Calculate factors
	var loadFactor float32
	var tombstoneFactor float32

	if totalSlots > 0 {
		loadFactor = float32(storedEntities) / float32(totalSlots)
		tombstoneFactor = float32(tombstones) / float32(totalSlots)
	}

	return FixedBlockMapInfo{
		LoadFactor:      loadFactor,
		TombstoneFactor: tombstoneFactor,
		RecommendGrow:   loadFactor >= 0.75,
		RecommendRehash: tombstoneFactor >= 0.20,
	}
}

// Rehash removes all deleted slots and rehashes all entries to optimize lookup performance.
// This function performs in-place rehashing without allocating additional memory for
// collecting entries, making it efficient for maps with millions of entries.
func (m *FixedBlockMap[V]) Rehash() error {
	//--==============================================================================--
	//--== Convert all deleted slots (0x1) to empty slots (0x0)
	//--==============================================================================--
	for blockIndex := range m.blocks {
		block := &m.blocks[blockIndex]
		for i := 0; i < FixedBlockSize; i++ {
			if block.controlByte(i) == 0x1 {
				block.setControlByte(i, 0x0)
			}
		}
	}

	//--==============================================================================--
	//--== Attempt to reposition entries not in their optimal blocks
	//--==============================================================================--

	type entry struct {
		key   FixedBlockKey
		value V
	}

	reinsertList := list.New()
	rehashedSinceLastReinsertAttempt := 0

	for blockIndex := range m.blocks {
		block := &m.blocks[blockIndex]

	BlockScanLoop:
		for i := 0; i < FixedBlockSize; i++ {
			// Skip empty slots
			if block.controlByte(i) == 0x0 {
				continue
			}

			key := block.keys[i]
			value := block.values[i]
			optimalBlockIndex := m.hashToBlock(key)
			currentBlockIndex := uint64(blockIndex)

			// Check if this entry is in its optimal position
			if optimalBlockIndex == currentBlockIndex {
				continue
			}

			// Check if optimal block has empty slots
			optimalBlock := &m.blocks[optimalBlockIndex]

			for j := 0; j < FixedBlockSize; j++ {
				if optimalBlock.controlByte(j) == 0x0 {
					tag := key[0] | 0x80

					// place the entry into the optimal block
					optimalBlock.setControlByte(j, tag)
					optimalBlock.keys[j] = key
					optimalBlock.values[j] = value

					// clear the current slot
					block.setControlByte(i, 0x0)

					// mark that we have successfully rehashed an entry
					rehashedSinceLastReinsertAttempt++

					continue BlockScanLoop
				}
			}

			// add this entry to be reinserted later
			reinsertList.PushBack(entry{
				key:   key,
				value: value,
			})

			// clear the slot in this block
			block.setControlByte(i, 0x0)
		}

		// Iterate through the reinsert list and try to place entries that can
		// now go into their optimal blocks (which may now have empty slots)
		if rehashedSinceLastReinsertAttempt > reinsertList.Len() {
			for e := reinsertList.Front(); e != nil; {
				next := e.Next() // Save next before potentially removing current
				entry := e.Value.(entry)
				optimalBlockIndex := m.hashToBlock(entry.key)
				optimalBlock := &m.blocks[optimalBlockIndex]

				// Check if optimal block now has empty slots
				for j := 0; j < FixedBlockSize; j++ {
					if optimalBlock.controlByte(j) == 0x0 {
						tag := entry.key[0] | 0x80

						// Place the entry into the optimal block
						optimalBlock.setControlByte(j, tag)
						optimalBlock.keys[j] = entry.key
						optimalBlock.values[j] = entry.value

						// Remove from list since it's been successfully reinserted
						reinsertList.Remove(e)
						break
					}
				}

				e = next // Move to next element
			}

			rehashedSinceLastReinsertAttempt = 0
		}
	}

	//--==============================================================================--
	//--== Insert any remaining entries that need to be reinserted
	//--==============================================================================--

	for reinsertList.Len() != 0 {
		entry := reinsertList.Remove(reinsertList.Front()).(entry)

		if err := m.Put(entry.key, entry.value); err != nil {
			return err
		}
	}

	return nil
}

// Grow increases the map capacity to the specified value. If the new capacity
// requires no additional blocks, the function returns early. The map never shrinks.
// All entries are rehashed to their optimal positions in the new structure,
// and all deleted slots (tombstones) are removed in the process.
// This operation extends the existing blocks slice in-place and then rehashes.
// Entries that need to be moved are collected first, then re-inserted, to avoid
// issues with entries being moved during iteration.
func (m *FixedBlockMap[V]) Grow(newCapacity uint64) error {
	newBlockCount := calculateBlockCount(newCapacity)
	currentBlockCount := uint64(len(m.blocks))

	// Early return if no growth needed (never shrink)
	if newBlockCount <= currentBlockCount {
		return nil
	}

	// Calculate how many new blocks to add
	blocksToAdd := int(newBlockCount - currentBlockCount)

	// Extend the existing blocks slice by appending new empty blocks
	m.blocks = append(m.blocks, make([]FixedBlock[V], blocksToAdd)...)
	m.mask = newBlockCount - 1

	return m.Rehash()
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
