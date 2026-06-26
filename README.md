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

## StaticFlatMap

`StaticFlatMap` is a read-only map for integer keys, backed by two parallel slices: sorted keys and their corresponding values. It is built once from a standard Go `map` and optimized for cache-friendly lookups via binary search.

### Features

- **Immutable after construction**: No insert, update, or delete operations
- **Integer keys only**: Works with any signed or unsigned integer type
- **Sorted flat storage**: Keys are sorted at build time for O(log n) binary search lookups
- **Memory efficient**: Two contiguous slices with no hash table overhead

### Usage

```go
package main

import (
    "fmt"
    "github.com/schraf/collections"
)

func main() {
    data := map[uint64]uint32{
        42:          33,
        100:         85,
        500_000_000: 13,
    }

    m := collections.NewStaticFlatMap(data)

    value, found := m.Get(42)
    if found {
        fmt.Println(value) // 33
    }

    _, found = m.Get(999)
    if !found {
        fmt.Println("Key not found")
    }
}
```

### API Reference

#### `NewStaticFlatMap[KeyType Integer, ValueType any](data map[KeyType]ValueType) *StaticFlatMap[KeyType, ValueType]`

Builds a static map from an existing Go map. Keys are sorted once during construction.

#### `Get(key KeyType) (ValueType, bool)`

Looks up a value by key using binary search. Returns the value and a boolean indicating whether the key was found.

### Performance Characteristics

- **Lookup**: O(log n) via binary search on a sorted key slice
- **Construction**: O(n log n) for sorting keys
- **Memory**: Two slices of length n (keys and values)

### When to Use

`StaticFlatMap` is ideal for:
- Fixed key sets that are known at initialization time
- Read-heavy workloads with integer keys
- Cases where predictable memory layout matters more than O(1) hash lookups

Consider a standard Go `map` or `FixedBlockMap` for:
- Dynamic insertions and deletions
- String or non-integer keys
- Very large maps where O(log n) lookup cost matters

## Directed Graph

The `collections` package provides memory-efficient, dense directed and bidirected graph implementations.

### Features

- **Dense Node Indices**: Nodes are represented as dense `uint32` indices for cache-friendly memory access patterns.
- **Immutable and Mutable Variants**: Choose between read-only, high-performance graph structures (`DirectedGraph`) or mutable versions that support edge additions and deletions (`MutableDirectedGraph`).
- **Bidirectional Support**: Immutable and mutable bidirected graphs that maintain both outbound and inbound adjacency lists.
- **Edge Labels**: Every edge carries a `uint8` label, useful for typing relationships or associating metadata.
- **Builder-based Construction**: Accumulate edges using `GraphBuilder` and efficiently construct immutable `DirectedGraph` representations.

### Design

The base `DirectedGraph` is immutable and stores adjacencies in Compressed Sparse Row (CSR) form using three parallel arrays: `offsets`, `edges`, and `labels`. This guarantees optimal memory locality and zero garbage collection overhead per edge.

Each of the three arrays is held behind the `Array[T]` interface rather than as a raw slice. This allows the graph to be backed by different storage implementations, with the default `SliceArray` providing zero-overhead, zero-copy traversal over plain in-memory slices.

The `MutableDirectedGraph` wraps an immutable `DirectedGraph` and uses internal maps (`addedEdges`, `removedEdges`) to track additions and deletions. It combines these modifications on-the-fly during neighbor traversal, enabling fast edge mutations without rebuilding the underlying arrays.

### Usage

#### Building an Immutable Graph

```go
package main

import (
	"fmt"

	"github.com/schraf/collections"
)

func main() {
	nodeCount := uint32(5)
	builder := collections.NewGraphBuilder[uint64, uint8](nodeCount, true /* withLabels */)

	// Add edges: From, To, Label
	builder.AddEdgeWithLabel(0, 1, 10)
	builder.AddEdgeWithLabel(0, 2, 20)
	builder.AddEdgeWithLabel(1, 3, 30)

	// Build the outbound directed graph
	graph := builder.BuildOutboundGraph()

	// Traverse neighbors of node 0
	neighbors, labels, err := graph.Neighbors(0)
	if err != nil {
		panic(err)
	}
	for i, neighbor := range neighbors {
		fmt.Printf("0 -> %d (Label: %d)\n", neighbor, labels[i])
	}
}
```

#### Using Mutable Graphs

```go
package main

import (
	"fmt"

	"github.com/schraf/collections"
)

func main() {
	nodeCount := uint32(5)
	builder := collections.NewGraphBuilder[uint64, uint8](nodeCount, true /* withLabels */)
	builder.AddEdgeWithLabel(0, 1, 10)

	baseGraph := builder.BuildOutboundGraph()

	// Create a mutable wrapper around the immutable base graph
	mutableGraph := collections.NewMutableDirectedGraph(*baseGraph)

	// Add new edges
	mutableGraph.AddEdgeWithLabel(0, 2, 20)

	// Remove existing edges
	mutableGraph.RemoveEdge(0, 1)

	// Traversal will now reflect added and removed edges
	neighbors, labels, err := mutableGraph.Neighbors(0)
	if err != nil {
		panic(err)
	}
	for i, neighbor := range neighbors {
		fmt.Printf("0 -> %d (Label: %d)\n", neighbor, labels[i])
	}
}
```

### API Reference

#### `NewGraphBuilder[OffsetType, EdgeType](nodeCount uint32, withLabels bool) *GraphBuilder`

Creates a new graph builder pre-sized for the specified number of nodes. `OffsetType` is the unsigned integer type used for CSR offsets, and `EdgeType` is the per-edge label type. Pass `withLabels = false` to build an unlabeled graph.

#### `(*GraphBuilder).AddEdgeWithLabel(from uint32, to uint32, label EdgeType)`

Records a directed edge from the source to the destination node with a label.

#### `(*GraphBuilder).AddEdge(from uint32, to uint32)`

Records a directed edge without an explicit label (a zero label is stored when the graph is labeled).

#### `(*GraphBuilder).BuildOutboundGraph() *DirectedGraph`

Constructs an immutable directed graph keyed by source nodes (outbound edges).

#### `(*GraphBuilder).BuildInboundGraph() *DirectedGraph`

Constructs an immutable directed graph keyed by destination nodes (inbound edges).

#### `(*DirectedGraph).Neighbors(nodeId uint32) ([]uint32, []uint8, error)`

Returns the destination node IDs and their corresponding labels for the given node. The returned slices may be zero-copy references to the graph's internal arrays (depending on the `Array` backend) and must not be modified.

#### `(*DirectedGraph).NeighborsInto(nodeId uint32, edgeBuf []uint32, labelBuf []uint8) ([]uint32, []uint8, error)`

Copies the neighbors (and labels, when present) of `nodeId` into the supplied buffers, growing them as needed, and returns the filled sub-slices. Reusing the buffers across calls makes traversal allocation-free. Pass a `nil` `labelBuf` for unlabeled graphs.

#### `NewDirectedGraphFromArrays[OffsetType, EdgeType](offsets Array[OffsetType], edges Array[DenseId], labels Array[EdgeType]) *DirectedGraph`

Builds a directed graph from arbitrary `Array` backends. A `nil` `labels` array indicates an unlabeled graph. The slice-based `NewDirectedGraph` remains available and wraps its inputs in `SliceArray`s automatically.

#### `NewMutableDirectedGraph(base DirectedGraph) *MutableDirectedGraph`

Creates a mutable graph structure that wraps an existing immutable graph.

#### `(*MutableDirectedGraph).AddEdgeWithLabel(a uint32, b uint32, label EdgeType)` / `AddEdge(a uint32, b uint32)`

Adds a new edge to the mutable graph, with or without an explicit label. These operations are thread-safe using a read-write lock.

#### `(*MutableDirectedGraph).RemoveEdge(a uint32, b uint32)`

Removes an edge from the mutable graph. This operation is thread-safe.

#### `(*MutableDirectedGraph).Neighbors(nodeId uint32) ([]uint32, []uint8, error)`

Returns the neighbors, reflecting both edges from the base graph and any applied additions or removals.

#### `BidirectedGraph` & `MutableBidirectedGraph`

Containers for paired outbound and inbound graphs, offering `AddEdge` and `RemoveEdge` that automatically update both directions simultaneously.


### Performance Characteristics

- **Neighbor lookup (Immutable)**: O(1) array slice access, excellent cache locality.
- **Neighbor lookup (Mutable)**: Overheads associated with reading internal hash maps and tombstone filtering, plus a read lock.
- **Build**: Fast population of continuous memory buffers.

### When to Use

- When you have graphs with dense integer node IDs.
- When you need high-performance traversal with minimal memory overhead.
- When you need to incrementally mutate a large base graph (using `MutableDirectedGraph`).

## Array

`Array[T]` is a read-only abstraction over an indexable sequence of `T`. It decouples consumers (such as the directed graph) from where the backing data lives.

```go
type Array[T any] interface {
    Len() int
    At(index int) (T, error)
    Slice(start, end int, dst []T) ([]T, error)
}
```

An in-memory implementation is provided:

- **`SliceArray[T]`** — a zero-overhead wrapper over a plain Go slice. `Slice(start, end, nil)` returns a zero-copy view of the underlying storage; passing a non-`nil` `dst` copies the range into it.

## PagedArray

`PagedArray[T]` is a disk-backed, fixed-page array with a bounded in-RAM footprint maintained by an LRU page cache. It presents the abstraction of an indexable array whose backing data lives in a directory on disk. Pages are faulted into memory on access and evicted (least-recently-used first) once the configured max loaded pages limit is exceeded.

It is designed for environments such as containers with mounted external storage where an explicit, predictable memory ceiling is required and where OS page-cache backed `mmap` would not actually offload memory.

#### Features

- **Bounded memory**: resident memory is capped at a fixed number of pages, giving a hard, predictable ceiling regardless of data size.
- **Explicit LRU paging**: pages are read from disk on demand and evicted least-recently-used first. Dirty pages are written back on eviction.
- **Raw serialization**: elements are copied to and from disk as raw memory using `unsafe.Pointer`.
- **Concurrent safe**: safe for use by multiple goroutines via internal locking.

**Important**: Like `FixedBlockMap` serialization, `PagedArray` copies raw element memory. `T` must be a fixed-size value type with no pointers, slices, maps, or other reference types. Files are not portable across architectures with differing endianness or type layout.

#### Usage

```go
package main

import (
    "github.com/schraf/collections"
)

func main() {
    // Open a paged array in a directory, holding at most 1000 pages in memory.
    pa, err := collections.NewPagedArray[uint32]("./data_dir", 1000)
    if err != nil {
        panic(err)
    }
    defer pa.Close()

    // Write data to the paged array
    for i := 0; i < 1_000_000; i++ {
        if err := pa.Set(i, uint32(i)); err != nil {
            panic(err)
        }
    }

    // Read data back
    val, err := pa.At(500_000) // faults in the relevant page, copies out the value
    if err != nil {
        panic(err)
    }
    _ = val
}
```

#### API Reference

- `NewPagedArray[T any](directory string, maxLoadedPages int) (*PagedArray[T], error)` — creates or opens a paged array in the specified directory.
- `(*PagedArray[T]).Len() int` — returns the length of the array, tracked automatically during initialization and `Set` operations.
- `(*PagedArray[T]).At(index int) (T, error)` — reads the element at the given index.
- `(*PagedArray[T]).Set(index int, value T) error` — writes the element at the given index.
- `(*PagedArray[T]).Slice(start, end int, dest []T) ([]T, error)` — copies a contiguous range into `dest`.
- `(*PagedArray[T]).Flush() error` — explicitly flushes any dirty pages and metadata to disk.
- `(*PagedArray[T]).Close() error` — flushes any dirty pages/metadata and closes the open files.

#### Performance Characteristics

- **Resident memory**: hard-capped at the configured page limit; never grows with data size.
- **Cache hit**: fast lookup with a lock and value copy.
- **Cache miss**: disk read of a single page, with at most one eviction (and a flush if the evicted page is dirty).

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
