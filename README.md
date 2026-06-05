# Collections

A Go package providing specialized, high-performance collection data structures.

## Overview

This package contains optimized collection implementations designed for specific use cases where performance and memory efficiency are critical. The collections are built with modern Go generics and leverage low-level optimizations for maximum throughput.

## FixedBlockMap

`FixedBlockMap` is a high-performance hash map implementation that uses a fixed-block structure for cache-friendly memory access patterns. It's designed for scenarios where you need predictable performance and can pre-allocate capacity.

### Features

- **Fixed-block structure**: Data is organized into blocks of 8 entries, improving CPU cache locality
- **SIMD-friendly operations**: Uses bitwise operations for parallel tag matching within blocks
- **Open addressing with linear probing**: Handles collisions by probing to the next block
- **Tombstone-based deletion**: Deleted entries are marked for efficient reinsertion
- **Iteration support**: Iterate over all values using Go's range-over-func iterator
- **In-place rehashing**: Efficiently remove tombstones and optimize entry placement using a linked list for deferred entries
- **Dynamic growth**: Extend map capacity in-place and automatically rehash entries to optimal positions
- **Health monitoring**: Collect statistics and get recommendations for when to rehash or grow
- **Serialization support**: Can write/read the entire map structure directly to/from memory
- **Type-safe with generics**: Works with any value type using Go generics

**Important**: Due to the raw memory serialization (`WriteTo`/`ReadFrom`), value types must not contain pointers, slices, maps, or other reference types. Use only plain structs with primitive types, arrays, or other value types without indirection. Types like `string`, `[]byte`, or structs containing pointers will not serialize correctly.

### Design

The map uses a two-level hashing scheme:
1. **Block-level hashing**: The first 8 bytes of a 16-byte key determine which block to start searching
2. **Tag-based matching**: Each entry has a control byte (tag) that enables fast parallel matching within a block
3. **Full key comparison**: Only matching tags trigger a full 16-byte key comparison

Keys are 16-byte values derived from strings using xxHash with a mixer function to minimize collisions. The map size is always a power of two, enabling fast modulo operations via bitwise AND.

### Usage

```go
package main

import (
    "fmt"
    "github.com/schraf/collections"
)

// UserData is a struct containing only value types (no pointers, slices, or maps)
// This is required for proper serialization support
type UserData struct {
    ID    uint64
    Score int32
    Flags uint16
    Data  [4]byte
}

func main() {
    // Create a map with capacity for ~100 entries
    m := collections.NewFixedBlockMap[UserData](100)
    
    // Create keys from strings
    var key1, key2 collections.FixedBlockKey
    key1.FromString("user:123")
    key2.FromString("user:456")
    
    // Insert values
    err := m.Put(key1, UserData{
        ID:    123,
        Score: 1000,
        Flags: 0x01,
        Data:  [4]byte{1, 2, 3, 4},
    })
    if err != nil {
        panic(err)
    }
    
    m.Put(key2, UserData{
        ID:    456,
        Score: 2000,
        Flags: 0x02,
        Data:  [4]byte{5, 6, 7, 8},
    })
    
    // Retrieve values
    val, found := m.Get(key1)
    if found {
        fmt.Printf("User ID: %d, Score: %d\n", val.ID, val.Score)
    }
    
    // Update values
    m.Put(key1, UserData{
        ID:    123,
        Score: 1500, // Updated score
        Flags: 0x03,
        Data:  [4]byte{9, 10, 11, 12},
    })
    
    // Delete entries
    m.Delete(key2)
    
    // Check if key exists after deletion
    val, found = m.Get(key2)
    if !found {
        fmt.Println("Key not found")
    }
    
    // Iterate over all values
    for _, value := range m.Iter() {
        fmt.Printf("User ID: %d, Score: %d\n", value.ID, value.Score)
    }
    
    // Check map health and get recommendations
    info := m.CollectInfo()
    fmt.Printf("Load Factor: %.2f, Tombstone Factor: %.2f\n", info.LoadFactor, info.TombstoneFactor)
    
    // After many deletions, rehash to optimize performance
    if info.RecommendRehash {
        err = m.Rehash()
        if err != nil {
            panic(err)
        }
    }
    
    // Grow the map if it's getting full
    if info.RecommendGrow {
        err = m.Grow(m.Capacity() * 2)
        if err != nil {
            panic(err)
        }
    }
}
```

### API Reference

#### `NewFixedBlockMap[V any](capacity uint64) *FixedBlockMap[V]`

Creates a new map with the specified capacity. The actual number of blocks allocated will be rounded up to the next power of two based on the capacity.

#### `FixedBlockKey.FromString(text string)`

Converts a string into a 16-byte key using xxHash. The same string will always produce the same key.

#### `Get(key FixedBlockKey) (*V, bool)`

Retrieves a value by key. Returns a pointer to the value and a boolean indicating whether the key was found.

#### `Put(key FixedBlockKey, value V) error`

Inserts or updates a key-value pair. Returns an error if the map is full (all blocks are occupied).

#### `Delete(key FixedBlockKey)`

Removes a key from the map. The operation is idempotent - deleting a non-existent key is safe.

#### `Iter() iter.Seq[*V]`

Returns an iterator over all values in the map. Uses Go's range-over-func iterator pattern. Deleted entries are automatically skipped. The iteration order is not guaranteed.

```go
for key, value := range m.Iter() {
    // Process each value
    fmt.Printf("Key: %v, Value: %v\n", key, *value)
}
```

#### `Rehash() error`

Removes all deleted slots (tombstones) and rehashes all entries to their optimal positions. This improves lookup performance by eliminating tombstone interference and reducing probe chain lengths. The operation uses an efficient in-place algorithm that:

- Converts all deleted slots to empty slots
- Moves entries to their optimal blocks when space is available
- Periodically attempts to reinsert deferred entries as slots become available
- Uses a linked list to track entries that need reinsertion, avoiding memory overhead

**When to use**: Call `Rehash()` periodically after performing many deletions, especially if lookup performance has degraded. You can use `CollectInfo()` to check if rehashing is recommended. The function is safe to call at any time and will not affect existing entries.

```go
// After many deletions
err := m.Rehash()
if err != nil {
    // Handle error (should be rare, only if map is full during re-insertion)
}
```

#### `Grow(newCapacity uint64) error`

Increases the map capacity to the specified value. If the new capacity requires no additional blocks, the function returns early. The map never shrinks. The operation:

- Extends the existing blocks slice in-place (no separate allocation)
- Automatically calls `Rehash()` to rehash all entries to their optimal positions with the new mask
- Removes all tombstones in the process
- Efficiently handles maps with millions of entries

**When to use**: Call `Grow()` when you need more capacity. Use `CollectInfo()` to check if growing is recommended (when load factor is high). The function is safe to call at any time.

```go
// Grow to double the current capacity
err := m.Grow(m.Capacity() * 2)
if err != nil {
    // Handle error (should be rare, only if map is full during re-insertion)
}
```

#### `CollectInfo() FixedBlockMapInfo`

Collects statistics about the map and provides recommendations for optimization. Returns a `FixedBlockMapInfo` struct containing:

- **LoadFactor**: Ratio of stored entities to total capacity (0.0 to 1.0)
- **TombstoneFactor**: Ratio of deleted slots (tombstones) to total capacity (0.0 to 1.0)
- **RecommendRehash**: `true` when tombstone factor is >= 0.20, indicating rehashing would be beneficial
- **RecommendGrow**: `true` when load factor is >= 0.75, indicating the map is getting full

**When to use**: Call `CollectInfo()` periodically to monitor map health and decide when to call `Rehash()` or `Grow()`.

```go
info := m.CollectInfo()
fmt.Printf("Load: %.2f%%, Tombstones: %.2f%%\n", 
    info.LoadFactor*100, info.TombstoneFactor*100)

if info.RecommendRehash {
    m.Rehash()
}

if info.RecommendGrow {
    m.Grow(m.Capacity() * 2)
}
```

#### `WriteTo(w io.Writer) (int64, error)`

Writes the entire map structure to an `io.Writer`. This performs a raw memory dump, so the map can be efficiently serialized. **Warning**: Only use with value types that contain no pointers, slices, maps, or other reference types. Types with indirection (like `string`, `[]byte`, or structs with pointer fields) will not serialize correctly.

#### `ReadFrom(r io.Reader) (int64, error)`

Reads a map structure from an `io.Reader`. The map must be initialized with the correct capacity before calling this method. The value type must match the type used when writing, and must not contain any pointers or reference types.

### Performance Characteristics

- **Lookup**: O(1) average case, with excellent cache locality due to block structure
- **Insert**: O(1) average case, with automatic updates for existing keys
- **Delete**: O(1) average case, using tombstone markers
- **Memory**: Fixed allocation based on capacity (power of two block count)

### Limitations

- Initial capacity must be specified at creation time (can be extended later with `Grow()`)
- Map overflow error occurs when all blocks are full
- Keys must be created using `FromString` or manually constructed as 16-byte arrays
- Serialization requires the map to be initialized with matching capacity
- **Value types must not contain pointers, slices, maps, or other reference types** - use only plain structs with primitive types, arrays, or other value types without indirection

### When to Use

`FixedBlockMap` is ideal for:
- High-performance lookups with known capacity
- Scenarios where cache locality matters
- Applications requiring serialization of map state
- Use cases where you can pre-allocate based on expected size

Consider the standard Go `map` for:
- Automatic dynamic resizing (FixedBlockMap requires manual `Grow()` calls)
- String keys without conversion
- Simpler API needs

## CSR

`CSR` is a compact, immutable adjacency structure in compressed-sparse-row form, designed to hold the topology of very large directed graphs (tens of millions of nodes, hundreds of millions of edges) resident in memory at minimal cost.

### Features

- **Compressed-sparse-row layout**: edges are stored as three parallel arrays (`offsets`, `neighbors`, `labels`) over dense node indices, with no per-node or per-edge pointers — cache-friendly and GC-light
- **Generic**: parameterized over the external node identifier `N` and edge label `L` (both `comparable`)
- **Dictionary-encoded labels**: each distinct edge label is interned to a one-byte code, so labels cost a single byte per edge (up to `MaxCSRLabels` = 256 distinct labels)
- **Immutable after build**: a built `CSR` is safe for concurrent reads; reflect mutations by building a new one and swapping it in
- **Builder-based construction**: accumulate edges via a builder that assigns dense indices lazily, then `Build()` into the final compact form
- **Bidirectional view**: `BiCSR` holds outbound and inbound adjacencies over a shared dense-index space, the typical shape for graph traversal
- **Allocation-free iteration**: `Neighbors` returns a range-over-func iterator yielding `(neighborDense, labelCode)` pairs

### Design

A directed graph is stored over dense node indices `[0, N)`:

```
offsets   len N+1   offsets[i]..offsets[i+1] is node i's slice of edges
neighbors len E     destination dense index of each edge
labels    len E     dictionary-encoded edge label of each edge
```

External node ids are mapped to dense indices via an internal map, with a reverse slice (`denseToID`) for hydrating results. Edge labels are interned to `uint8` codes. A node's outgoing edges are the contiguous range `neighbors[offsets[i]:offsets[i+1]]` with parallel `labels`.

`BiCSR` builds two such structures from one edge stream so that a given external id resolves to the same dense index in both the outbound and inbound directions, allowing a traversal to carry dense indices across direction changes without re-resolving.

### Usage

```go
package main

import (
	"fmt"

	"github.com/schraf/collections"
)

func main() {
	// Build a bidirectional graph: 1 -> 2 -> 3, plus 1 -> 3.
	b := collections.NewBiCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "depends_on")
	b.AddEdge(2, 3, "depends_on")
	b.AddEdge(1, 3, "built_from")

	g, err := b.Build()
	if err != nil {
		panic(err)
	}

	// Traverse outbound neighbors of node 1, filtering by label.
	start, ok := g.Dense(1)
	if !ok {
		panic("node 1 missing")
	}

	for nb, label := range g.Out().Neighbors(start) {
		fmt.Printf("1 -%s-> %d\n", g.Out().Label(label), g.Out().ID(nb))
	}

	// Inbound neighbors of node 3 (who points at it).
	d3, _ := g.In().Dense(3)
	for nb := range g.In().Neighbors(d3) {
		fmt.Printf("%d -> 3\n", g.In().ID(nb))
	}
}
```

### API Reference

#### `NewCSRBuilder[N, L comparable](nodeHint, edgeHint int) *CSRBuilder[N, L]`

Creates a single-direction builder. The hints pre-size internal buffers (0 is fine).

#### `(*CSRBuilder).AddNode(id N) int32`

Ensures `id` has a dense index (for registering isolated nodes), returning it.

#### `(*CSRBuilder).AddEdge(src, dst N, label L)`

Records a directed edge. Endpoints are registered if new. Errors (e.g. exceeding `MaxCSRLabels`) are recorded and surfaced by `Build`.

#### `(*CSRBuilder).Build() (*CSR[N, L], error)`

Produces the immutable `CSR` and releases the builder's accumulation buffers. The builder must not be reused afterward.

#### `NewBiCSRBuilder[N, L comparable](nodeHint, edgeHint int) *BiCSRBuilder[N, L]`

Creates a bidirectional builder with the same `AddNode`/`AddEdge`/`Build` API, producing a `BiCSR`.

#### `(*CSR).Dense(id N) (int32, bool)` / `(*CSR).ID(dense int32) N`

Convert between external node ids and dense indices.

#### `(*CSR).LabelCode(label L) (uint8, bool)` / `(*CSR).Label(code uint8) L`

Convert between external labels and internal codes.

#### `(*CSR).Degree(dense int32) int`

Number of outgoing edges of a dense node.

#### `(*CSR).Row(dense int32) (neighbors []int32, labels []uint8)`

Returns the neighbor and label slices for a node. The slices alias internal arrays and must not be modified.

#### `(*CSR).Neighbors(dense int32) func(yield func(neighbor int32, label uint8) bool)`

Returns a range-over-func iterator over a node's edges, allocation-free and break-able.

#### `(*BiCSR).Out() *CSR[N, L]` / `(*BiCSR).In() *CSR[N, L]`

The outbound (keyed on source) and inbound (keyed on target) adjacencies, sharing a dense-index space.

### Performance Characteristics

- **Neighbor lookup**: O(degree), sequential scan of contiguous arrays with excellent cache locality
- **Build**: O(V + E) via degree counting and a prefix-sum fill, single allocation per array
- **Memory**: roughly `(N+1 + E)·4` bytes for offsets+neighbors plus `E` bytes for labels, per direction, plus the id↔dense maps

### Limitations

- Immutable after `Build`; mutations require rebuilding
- At most `MaxCSRLabels` (256) distinct edge labels, due to one-byte label encoding
- Dense indices are `int32`, bounding a single CSR to ~2.1 billion nodes/edges

### When to Use

`CSR` is ideal for:
- Holding a large, read-mostly graph topology resident in memory
- Repeated traversals (BFS/DFS) where per-hop neighbor lookups must be fast
- Workloads where graph structure changes infrequently (rebuild-and-swap)

Consider an adjacency-list or map-of-slices for:
- Frequently mutated graphs
- Small graphs where construction overhead and immutability are not worth it

## StringArena

`StringArena` is a high-performance, chunk-based memory allocator optimized for allocating strings. It consolidates many small, transient allocations into large, contiguous blocks (chunks) and uses zero-copy string views, which dramatically reduces Garbage Collector (GC) pressure and CPU overhead in high-throughput workloads.

### Features

- **Chunk-based allocation**: Accumulates string copies in pre-allocated contiguous buffers, minimizing heap fragmentation
- **Zero-allocation conversions**: Employs unsafe pointers (`unsafe.String`) to produce string headers referencing the arena memory directly, completely avoiding standard Go string heap allocations on allocate
- **Automatic dynamic growth**: Allocates new chunks as memory demands increase, while keeping all previous string views completely valid
- **Instant resets and reuse**: Fast reset operation that enables full chunk reuse across operations without reallocating underlying heap memory
- **High performance**: Yields zero allocations and massive speedups for strings larger than 128 bytes in comparative benchmarks
- **Statistics reporting**: Provides granular insights into total capacity, bytes used, and chunk counts

### Design

A `StringArena` pre-allocates an initial byte slice (chunk) of a given size. When a string or byte slice is allocated into the arena:
1. It copies the source data into the current active chunk.
2. It returns a string that directly points to this segment of the chunk using Go 1.20+ standard `unsafe.String` functionality.
3. If the active chunk becomes full, the arena automatically allocates a new chunk (using the larger of the default chunk size or the size of the incoming string).
4. When `Reset()` is called, the arena sets its indices back to zero, allowing previous chunks to be completely overwritten and reused without releasing or reallocating the underlying memory.

### Usage

```go
package main

import (
	"fmt"
	"github.com/schraf/collections"
)

func main() {
	// Initialize an arena with a 16KB chunk size
	arena := collections.NewStringArena(16384)

	// Allocate transient strings or byte slices into the arena
	s1 := arena.Alloc("hello")
	s2 := arena.AllocBytes([]byte("world"))

	fmt.Printf("%s %s\n", s1, s2) // Output: hello world

	// Check stats
	stats := arena.Stats()
	fmt.Printf("Chunks: %d, Allocated: %d, Used: %d\n", stats.NumChunks, stats.TotalAllocated, stats.TotalUsed)

	// Reset the arena for reuse (invalidates previously allocated strings)
	arena.Reset()
}
```

### API Reference

#### `NewStringArena(chunkSize int) *StringArena`

Creates a new string arena with the specified chunk size in bytes. If `chunkSize` is non-positive, it defaults to `16384` (16KB). If it is less than `1024`, it is enforced to a minimum of `1024` (1KB) to prevent small, inefficient chunks.

#### `(*StringArena).Alloc(s string) string`

Copies the content of the transient string `s` into the arena and returns a string view pointing directly to the arena's memory. If `s` is empty, it returns `""` without modifying the arena.

#### `(*StringArena).AllocBytes(b []byte) string`

Copies the content of the transient byte slice `b` into the arena and returns a string view pointing directly to the arena's memory. If `b` is empty, it returns `""` without modifying the arena.

#### `(*StringArena).Reset()`

Resets the arena's allocation index back to the beginning. All previously allocated chunks are preserved for reuse. Calling `Reset()` invalidates all previously allocated strings from this arena, and their memory might be overwritten during subsequent allocations.

#### `(*StringArena).Stats() Stats`

Returns a `Stats` struct with the following fields:
- **TotalAllocated**: Total bytes of heap memory allocated across all chunks.
- **TotalUsed**: Active bytes currently used by strings in the current allocation cycle.
- **NumChunks**: The number of chunks allocated.

### Performance Characteristics

In typical workloads involving string parsing (such as JSON or CSV parsing):
- **Allocate**: O(1) average case, with near-zero overhead consisting of a single memory copy of the string contents.
- **Garbage Collection**: Reduces GC overhead to a single object (the arena itself) rather than millions of independent string allocations.
- **Growth**: O(1) chunk allocation that doesn't copy previously allocated data.

### Limitations

- **Concurrent Safety**: `StringArena` is not safe for concurrent use by multiple goroutines. External synchronization must be provided if used across goroutines.
- **Memory Lifetime**: Strings allocated from the arena remain valid until `Reset()` is called (provided the arena is not garbage collected). If individual strings need to live longer than others, a general arena may not be appropriate.

### When to Use

`StringArena` is ideal for:
- High-performance parsers (JSON, CSV, protocols) where transient bytes are converted to long-lived strings
- Compilers, interpreters, or key-value stores with intensive string-interning requirements
- Batch-processing tasks where memory can be allocated, processed, and completely reset at the end of each batch

Consider standard Go strings for:
- Long-lived strings with individual, independent lifetimes
- Scenarios where concurrent goroutines share the allocator without synchronization

## License

[See LICENSE file](LICENSE)
