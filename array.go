package collections

// Array is a read-only, indexable sequence of T. It abstracts over where the
// backing data lives, allowing consumers to be agnostic to whether the data is
// held entirely in RAM (SliceArray) or paged on and off disk (PagedArray).
//
// Implementations must be safe for concurrent readers.
type Array[T any] interface {
	Len() int
	At(index int) (T, error)
	Slice(start int, end int, dest []T) ([]T, error)
}

// SliceArray is an in-memory Array[T] backed by a plain Go slice. It adds no
// overhead over direct slice access and preserves zero-copy semantics.
type SliceArray[T any] struct {
	data []T
}

func NewSliceArray[T any](data []T) *SliceArray[T] {
	return &SliceArray[T]{data: data}
}

func (a *SliceArray[T]) Len() int {
	return len(a.data)
}

func (a *SliceArray[T]) At(index int) (T, error) {
	return a.data[index], nil
}

func (a *SliceArray[T]) Slice(start int, end int, dest []T) ([]T, error) {
	if dest == nil {
		return a.data[start:end], nil
	}

	count := end - start
	if cap(dest) < count {
		dest = make([]T, count)
	}

	dest = dest[:count]
	copy(dest, a.data[start:end])
	return dest, nil
}
