package collections

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testValue is a struct type used for testing that contains no pointers
// This is the typical use case for FixedBlockMap since it supports serialization
type testValue struct {
	ID    uint64
	Score int32
	Flags uint16
	Data  [4]byte
}

func TestFixedBlockKey_FromString(t *testing.T) {
	var key1, key2 FixedBlockKey
	key1.FromString("hello")
	key2.FromString("world")

	// Different strings should produce different keys
	assert.NotEqual(t, key1, key2)

	// Same string should produce same key
	var key3 FixedBlockKey
	key3.FromString("hello")
	assert.Equal(t, key1, key3)
}

func TestNewFixedBlockMap(t *testing.T) {
	// Test with small capacity
	m := NewFixedBlockMap[testValue](10)
	require.NotNil(t, m)
	assert.NotNil(t, m.blocks)
	assert.Greater(t, len(m.blocks), 1)
	assert.GreaterOrEqual(t, m.Capacity(), uint64(10))

	// Test with larger capacity
	m2 := NewFixedBlockMap[testValue](100)
	require.NotNil(t, m2)
	assert.Greater(t, len(m2.blocks), 1)
	assert.GreaterOrEqual(t, m2.Capacity(), uint64(100))
}

func TestFixedBlockMap_PutAndGet(t *testing.T) {
	m := NewFixedBlockMap[testValue](10)

	var key1, key2 FixedBlockKey
	key1.FromString("key1")
	key2.FromString("key2")

	// Put first key-value pair
	value1 := testValue{ID: 1, Score: 100, Flags: 0x01, Data: [4]byte{1, 2, 3, 4}}
	err := m.Put(key1, value1)
	require.NoError(t, err)

	// Get it back
	val, found := m.Get(key1)
	require.True(t, found)
	require.NotNil(t, val)
	assert.Equal(t, value1, *val)

	// Put second key-value pair
	value2 := testValue{ID: 2, Score: 200, Flags: 0x02, Data: [4]byte{5, 6, 7, 8}}
	err = m.Put(key2, value2)
	require.NoError(t, err)

	// Get both values
	val1, found1 := m.Get(key1)
	require.True(t, found1)
	assert.Equal(t, value1, *val1)

	val2, found2 := m.Get(key2)
	require.True(t, found2)
	assert.Equal(t, value2, *val2)

	// Update existing key
	updatedValue1 := testValue{ID: 1, Score: 150, Flags: 0x03, Data: [4]byte{9, 10, 11, 12}}
	err = m.Put(key1, updatedValue1)
	require.NoError(t, err)

	val1, found1 = m.Get(key1)
	require.True(t, found1)
	assert.Equal(t, updatedValue1, *val1)

	// Verify other key unchanged
	val2, found2 = m.Get(key2)
	require.True(t, found2)
	assert.Equal(t, value2, *val2)
}

func TestFixedBlockMap_GetNonExistent(t *testing.T) {
	m := NewFixedBlockMap[testValue](10)

	var key1, key2 FixedBlockKey
	key1.FromString("key1")
	key2.FromString("key2")

	// Put one key
	value1 := testValue{ID: 1, Score: 100, Flags: 0x01, Data: [4]byte{1, 2, 3, 4}}
	err := m.Put(key1, value1)
	require.NoError(t, err)

	// Try to get non-existent key
	val, found := m.Get(key2)
	assert.False(t, found)
	assert.Nil(t, val)

	// Get from empty map
	m2 := NewFixedBlockMap[testValue](10)
	val2, found2 := m2.Get(key1)
	assert.False(t, found2)
	assert.Nil(t, val2)
}

func TestFixedBlockMap_Delete(t *testing.T) {
	m := NewFixedBlockMap[testValue](10)

	var key1, key2 FixedBlockKey
	key1.FromString("key1")
	key2.FromString("key2")

	// Put two keys
	value1 := testValue{ID: 1, Score: 100, Flags: 0x01, Data: [4]byte{1, 2, 3, 4}}
	err := m.Put(key1, value1)
	require.NoError(t, err)
	value2 := testValue{ID: 2, Score: 200, Flags: 0x02, Data: [4]byte{5, 6, 7, 8}}
	err = m.Put(key2, value2)
	require.NoError(t, err)

	// Verify both exist
	val1, found1 := m.Get(key1)
	require.True(t, found1)
	assert.Equal(t, value1, *val1)

	val2, found2 := m.Get(key2)
	require.True(t, found2)
	assert.Equal(t, value2, *val2)

	// Delete one key
	m.Delete(key1)

	// Verify deleted key is gone
	val, found := m.Get(key1)
	assert.False(t, found)
	assert.Nil(t, val)

	// Verify other key still exists
	val2, found2 = m.Get(key2)
	require.True(t, found2)
	assert.Equal(t, value2, *val2)

	// Delete non-existent key (should not panic)
	m.Delete(key1)

	// Delete the remaining key
	m.Delete(key2)
	val, found = m.Get(key2)
	assert.False(t, found)
	assert.Nil(t, val)
}

func TestFixedBlockMap_DeleteAndReinsert(t *testing.T) {
	m := NewFixedBlockMap[testValue](10)

	var key FixedBlockKey
	key.FromString("test_key")

	// Put, delete, then put again
	value1 := testValue{ID: 1, Score: 100, Flags: 0x01, Data: [4]byte{1, 2, 3, 4}}
	err := m.Put(key, value1)
	require.NoError(t, err)

	m.Delete(key)

	val, found := m.Get(key)
	assert.False(t, found)
	assert.Nil(t, val)

	// Reinsert after delete
	value2 := testValue{ID: 2, Score: 200, Flags: 0x02, Data: [4]byte{5, 6, 7, 8}}
	err = m.Put(key, value2)
	require.NoError(t, err)

	val, found = m.Get(key)
	require.True(t, found)
	assert.Equal(t, value2, *val)
}

func TestFixedBlockMap_MultipleOperations(t *testing.T) {
	m := NewFixedBlockMap[testValue](50)

	// Insert multiple keys
	keys := make([]FixedBlockKey, 20)
	for i := 0; i < 20; i++ {
		keys[i].FromString(string(rune('a' + i)))
		value := testValue{ID: uint64(i), Score: int32(i * 10), Flags: uint16(i), Data: [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)}}
		err := m.Put(keys[i], value)
		require.NoError(t, err)
	}

	// Verify all keys exist with correct values
	for i := 0; i < 20; i++ {
		val, found := m.Get(keys[i])
		require.True(t, found)
		expected := testValue{ID: uint64(i), Score: int32(i * 10), Flags: uint16(i), Data: [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)}}
		assert.Equal(t, expected, *val)
	}

	// Update some values
	updated0 := testValue{ID: 0, Score: 999, Flags: 0xFF, Data: [4]byte{99, 99, 99, 99}}
	m.Put(keys[0], updated0)
	updated10 := testValue{ID: 10, Score: 888, Flags: 0xEE, Data: [4]byte{88, 88, 88, 88}}
	m.Put(keys[10], updated10)

	// Verify updates
	val, found := m.Get(keys[0])
	require.True(t, found)
	assert.Equal(t, updated0, *val)

	val, found = m.Get(keys[10])
	require.True(t, found)
	assert.Equal(t, updated10, *val)

	// Delete some keys
	m.Delete(keys[5])
	m.Delete(keys[15])

	// Verify deletions
	val, found = m.Get(keys[5])
	assert.False(t, found)

	val, found = m.Get(keys[15])
	assert.False(t, found)

	// Verify others still exist
	val, found = m.Get(keys[0])
	require.True(t, found)
	assert.Equal(t, updated0, *val)
}

func TestFixedBlockMap_WriteToAndReadFrom(t *testing.T) {
	m1 := NewFixedBlockMap[testValue](10)

	var key1, key2, key3 FixedBlockKey
	key1.FromString("key1")
	key2.FromString("key2")
	key3.FromString("key3")

	// Put some values
	value1 := testValue{ID: 1, Score: 100, Flags: 0x01, Data: [4]byte{1, 2, 3, 4}}
	err := m1.Put(key1, value1)
	require.NoError(t, err)
	value2 := testValue{ID: 2, Score: 200, Flags: 0x02, Data: [4]byte{5, 6, 7, 8}}
	err = m1.Put(key2, value2)
	require.NoError(t, err)
	value3 := testValue{ID: 3, Score: 300, Flags: 0x03, Data: [4]byte{9, 10, 11, 12}}
	err = m1.Put(key3, value3)
	require.NoError(t, err)

	// Write to buffer
	var buf bytes.Buffer
	written, err := m1.WriteTo(&buf)
	require.NoError(t, err)
	assert.Greater(t, written, int64(0))

	// Create new map with same capacity and read from buffer
	m2 := NewFixedBlockMap[testValue](10)
	read, err := m2.ReadFrom(&buf)
	require.NoError(t, err)
	assert.Equal(t, written, read)

	// Verify all values are preserved
	val1, found1 := m2.Get(key1)
	require.True(t, found1)
	assert.Equal(t, value1, *val1)

	val2, found2 := m2.Get(key2)
	require.True(t, found2)
	assert.Equal(t, value2, *val2)

	val3, found3 := m2.Get(key3)
	require.True(t, found3)
	assert.Equal(t, value3, *val3)
}

func TestFixedBlockMap_Iter(t *testing.T) {
	m := NewFixedBlockMap[testValue](50)

	// Insert multiple values
	keys := make([]FixedBlockKey, 10)
	expectedValues := make([]testValue, 10)
	for i := 0; i < 10; i++ {
		keys[i].FromString(fmt.Sprintf("key%d", i))
		expectedValues[i] = testValue{
			ID:    uint64(i),
			Score: int32(i * 10),
			Flags: uint16(i),
			Data:  [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)},
		}
		err := m.Put(keys[i], expectedValues[i])
		require.NoError(t, err)
	}

	// Collect all values from iterator
	var iteratedValues []testValue
	for _, v := range m.Iter() {
		iteratedValues = append(iteratedValues, *v)
	}

	// Should have found all 10 values
	assert.Equal(t, 10, len(iteratedValues))

	// Verify all expected values are present (order may vary)
	found := make(map[testValue]bool)
	for _, v := range iteratedValues {
		found[v] = true
	}
	for _, expected := range expectedValues {
		assert.True(t, found[expected], "Expected value %v not found in iteration", expected)
	}

	// Delete some values and verify they're not in iteration
	m.Delete(keys[2])
	m.Delete(keys[5])
	m.Delete(keys[8])

	// Iterate again
	iteratedValues = nil
	for _, v := range m.Iter() {
		iteratedValues = append(iteratedValues, *v)
	}

	// Should have 7 values now (10 - 3 deleted)
	assert.Equal(t, 7, len(iteratedValues))

	// Verify deleted values are not present
	for _, v := range iteratedValues {
		assert.NotEqual(t, expectedValues[2], v)
		assert.NotEqual(t, expectedValues[5], v)
		assert.NotEqual(t, expectedValues[8], v)
	}

	// Verify remaining values are present
	found = make(map[testValue]bool)
	for _, v := range iteratedValues {
		found[v] = true
	}
	for i, expected := range expectedValues {
		if i != 2 && i != 5 && i != 8 {
			assert.True(t, found[expected], "Expected value %v not found in iteration", expected)
		}
	}
}

func TestFixedBlockMap_Rehash(t *testing.T) {
	m := NewFixedBlockMap[testValue](50)

	// Insert multiple keys
	keys := make([]FixedBlockKey, 20)
	expectedValues := make([]testValue, 20)
	for i := 0; i < 20; i++ {
		keys[i].FromString(fmt.Sprintf("key%d", i))
		expectedValues[i] = testValue{
			ID:    uint64(i),
			Score: int32(i * 10),
			Flags: uint16(i),
			Data:  [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)},
		}
		err := m.Put(keys[i], expectedValues[i])
		require.NoError(t, err)
	}

	// Delete some keys to create tombstone slots
	m.Delete(keys[3])
	m.Delete(keys[7])
	m.Delete(keys[11])
	m.Delete(keys[15])

	// Verify deleted keys are gone
	val, found := m.Get(keys[3])
	assert.False(t, found)
	assert.Nil(t, val)

	// Verify some remaining keys still exist
	val, found = m.Get(keys[0])
	require.True(t, found)
	assert.Equal(t, expectedValues[0], *val)

	val, found = m.Get(keys[10])
	require.True(t, found)
	assert.Equal(t, expectedValues[10], *val)

	// Perform rehash
	err := m.Rehash()
	require.NoError(t, err)

	// Verify all remaining entries are still accessible after rehash
	for i := 0; i < 20; i++ {
		if i == 3 || i == 7 || i == 11 || i == 15 {
			// Deleted entries should still be gone
			val, found = m.Get(keys[i])
			assert.False(t, found, "Deleted key %d should not be found after rehash", i)
			assert.Nil(t, val)
		} else {
			// All other entries should still be present
			val, found = m.Get(keys[i])
			require.True(t, found, "Key %d should be found after rehash", i)
			assert.Equal(t, expectedValues[i], *val, "Key %d should have correct value after rehash", i)
		}
	}

	// Verify iteration still works correctly after rehash
	var iteratedValues []testValue
	for _, v := range m.Iter() {
		iteratedValues = append(iteratedValues, *v)
	}
	// Should have 16 values (20 - 4 deleted)
	assert.Equal(t, 16, len(iteratedValues))

	// Verify we can still insert new entries after rehash
	var newKey FixedBlockKey
	newKey.FromString("new_key_after_rehash")
	newValue := testValue{ID: 999, Score: 999, Flags: 0xFF, Data: [4]byte{99, 99, 99, 99}}
	err = m.Put(newKey, newValue)
	require.NoError(t, err)

	val, found = m.Get(newKey)
	require.True(t, found)
	assert.Equal(t, newValue, *val)
}

func TestFixedBlockMap_Grow(t *testing.T) {
	// Test 1: Grow from small to large capacity
	m := NewFixedBlockMap[testValue](10)
	initialCapacity := m.Capacity()

	// Insert some entries
	keys := make([]FixedBlockKey, 5)
	expectedValues := make([]testValue, 5)
	for i := 0; i < 5; i++ {
		keys[i].FromString(fmt.Sprintf("key%d", i))
		expectedValues[i] = testValue{
			ID:    uint64(i),
			Score: int32(i * 10),
			Flags: uint16(i),
			Data:  [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)},
		}
		err := m.Put(keys[i], expectedValues[i])
		require.NoError(t, err)
	}

	// Delete one entry to create a tombstone
	m.Delete(keys[2])

	// Grow the map
	newCapacity := uint64(100)
	err := m.Grow(newCapacity)
	require.NoError(t, err)

	// Verify capacity increased
	assert.GreaterOrEqual(t, m.Capacity(), newCapacity)
	assert.Greater(t, m.Capacity(), initialCapacity)

	// Verify all non-deleted entries are still accessible
	for i := 0; i < 5; i++ {
		if i == 2 {
			// Deleted entry should still be gone
			val, found := m.Get(keys[i])
			assert.False(t, found, "Deleted key %d should not be found after grow", i)
			assert.Nil(t, val)
		} else {
			// All other entries should still be present
			val, found := m.Get(keys[i])
			require.True(t, found, "Key %d should be found after grow", i)
			assert.Equal(t, expectedValues[i], *val, "Key %d should have correct value after grow", i)
		}
	}

	// Verify iteration works
	var iteratedValues []testValue
	for _, v := range m.Iter() {
		iteratedValues = append(iteratedValues, *v)
	}
	// Should have 4 values (5 - 1 deleted)
	assert.Equal(t, 4, len(iteratedValues))

	// Test 2: Grow with no change (should early return)
	m2 := NewFixedBlockMap[testValue](100)
	initialCapacity2 := m2.Capacity()

	// Insert some entries
	var key1, key2 FixedBlockKey
	key1.FromString("key1")
	key2.FromString("key2")
	m2.Put(key1, testValue{ID: 1, Score: 100, Flags: 0x01, Data: [4]byte{1, 2, 3, 4}})
	m2.Put(key2, testValue{ID: 2, Score: 200, Flags: 0x02, Data: [4]byte{5, 6, 7, 8}})

	// Try to grow to a smaller capacity (should not shrink)
	err = m2.Grow(50)
	require.NoError(t, err)
	assert.Equal(t, initialCapacity2, m2.Capacity(), "Capacity should not shrink")

	// Try to grow to same capacity (should early return)
	err = m2.Grow(100)
	require.NoError(t, err)
	assert.Equal(t, initialCapacity2, m2.Capacity(), "Capacity should not change")

	// Verify entries still accessible
	val, found := m2.Get(key1)
	require.True(t, found)
	assert.Equal(t, uint64(1), val.ID)

	val, found = m2.Get(key2)
	require.True(t, found)
	assert.Equal(t, uint64(2), val.ID)

	// Test 3: Grow with many entries and deletions
	m3 := NewFixedBlockMap[testValue](50)
	keys3 := make([]FixedBlockKey, 30)
	expectedValues3 := make([]testValue, 30)
	for i := 0; i < 30; i++ {
		keys3[i].FromString(fmt.Sprintf("grow_key%d", i))
		expectedValues3[i] = testValue{
			ID:    uint64(i),
			Score: int32(i * 10),
			Flags: uint16(i),
			Data:  [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)},
		}
		err := m3.Put(keys3[i], expectedValues3[i])
		require.NoError(t, err)
	}

	// Delete several entries
	m3.Delete(keys3[5])
	m3.Delete(keys3[10])
	m3.Delete(keys3[15])
	m3.Delete(keys3[20])
	m3.Delete(keys3[25])

	// Grow to larger capacity
	err = m3.Grow(200)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, m3.Capacity(), uint64(200))

	// Verify all non-deleted entries are still accessible
	deletedIndices := map[int]bool{5: true, 10: true, 15: true, 20: true, 25: true}
	for i := 0; i < 30; i++ {
		if deletedIndices[i] {
			val, found := m3.Get(keys3[i])
			assert.False(t, found, "Deleted key %d should not be found after grow", i)
			assert.Nil(t, val)
		} else {
			val, found := m3.Get(keys3[i])
			require.True(t, found, "Key %d should be found after grow", i)
			assert.Equal(t, expectedValues3[i], *val, "Key %d should have correct value after grow", i)
		}
	}

	// Verify we can insert new entries after grow
	var newKey FixedBlockKey
	newKey.FromString("new_key_after_grow")
	newValue := testValue{ID: 999, Score: 999, Flags: 0xFF, Data: [4]byte{99, 99, 99, 99}}
	err = m3.Put(newKey, newValue)
	require.NoError(t, err)

	val, found = m3.Get(newKey)
	require.True(t, found)
	assert.Equal(t, newValue, *val)
}

func BenchmarkFixedBlockMap_Get(b *testing.B) {
	m := NewFixedBlockMap[testValue](100000)

	// Pre-populate the map with 50000 entries
	keys := make([]FixedBlockKey, 50000)
	for i := 0; i < 50000; i++ {
		keys[i].FromString(fmt.Sprintf("bench_key_%d", i))
		value := testValue{ID: uint64(i), Score: int32(i * 10), Flags: uint16(i), Data: [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)}}
		m.Put(keys[i], value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Access keys in a pattern to simulate real workload
		key := keys[i%50000]
		m.Get(key)
	}
}

func BenchmarkFixedBlockMap_Put(b *testing.B) {
	m := NewFixedBlockMap[testValue](uint64(b.N) + 1000)

	keys := make([]FixedBlockKey, b.N)
	for i := 0; i < b.N; i++ {
		keys[i].FromString(fmt.Sprintf("bench_key_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := testValue{ID: uint64(i), Score: int32(i * 10), Flags: uint16(i), Data: [4]byte{byte(i), byte(i + 1), byte(i + 2), byte(i + 3)}}
		m.Put(keys[i], value)
	}
}
