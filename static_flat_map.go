package collections

import (
	"slices"
)

type StaticFlatMap[KeyType Integer, ValueType any] struct {
	keys   []KeyType
	values []ValueType
}

func NewStaticFlatMap[KeyType Integer, ValueType any](data map[KeyType]ValueType) *StaticFlatMap[KeyType, ValueType] {
	size := len(data)

	keys := make([]KeyType, 0, size)
	for key := range data {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	values := make([]ValueType, size)

	for index, key := range keys {
		values[index] = data[key]
	}

	return &StaticFlatMap[KeyType, ValueType]{
		keys:   keys,
		values: values,
	}
}

func (m *StaticFlatMap[KeyType, ValueType]) Get(key KeyType) (ValueType, bool) {
	index, found := slices.BinarySearch(m.keys, key)
	if !found {
		var value ValueType
		return value, false
	}

	return m.values[index], true
}
