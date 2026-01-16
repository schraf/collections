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
    for value := range m.Iter() {
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
for value := range m.Iter() {
    // Process each value
    fmt.Printf("Value: %v\n", *value)
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

## License

[See LICENSE file](LICENSE)
