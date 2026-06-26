package collections

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	PageSize           = 4096    // 4 KB pages
	SegmentSize uint64 = 1 << 30 // 1 GB file segment
)

type Page struct {
	key   uint64
	data  []byte
	dirty bool
}

type PagedArrayMeta struct {
	Version     int    `json:"version"`
	Length      int    `json:"length"`
	ElementSize uint64 `json:"element_size"`
}

// PagedArray is a disk-backed, fixed-page array of T with a bounded in-RAM
// footprint maintained by an LRU page cache. It presents the abstraction of an
// indexable []T whose backing data lives in a file on disk; pages are faulted
// into memory on access and evicted (least-recently-used first) once the
// configured RAM budget is exceeded.
//
// PagedArray is designed for environments (such as containers with mounted
// external storage) where an explicit, predictable memory ceiling is required
// and where OS page-cache backed mmap would not actually offload memory.
//
// Element type constraint: PagedArray copies raw element memory to and from
// disk. T must therefore be a fixed-size value type that contains no pointers,
// slices, maps, or other reference types.  Types such as string, []byte, or
// structs with pointer fields are not supported. Files are not portable across
// architectures with differing endianness or type layout.
//
// PagedArray is safe for concurrent use by multiple goroutines.
type PagedArray[T any] struct {
	lock sync.Mutex

	directory string
	files     map[uint64]*os.File
	cache     *lru.Cache[uint64, *Page]

	elementSize     uint64
	elementsPerPage uint64
	pagesPerSegment uint64

	length int
	dirty  bool
}

func NewPagedArray[T any](directory string, maxLoadedPages int) (*PagedArray[T], error) {
	var zero T
	elementSize := uint64(unsafe.Sizeof(zero))

	if elementSize > PageSize {
		return nil, fmt.Errorf("type size (%d bytes) exceeds maximum PageSize (%d bytes)", elementSize, PageSize)
	}

	elementsPerPage := uint64(PageSize) / elementSize
	pagesPerSegment := SegmentSize / PageSize

	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	metaPath := filepath.Join(directory, "meta.json")
	metaBytes, err := os.ReadFile(metaPath)
	dirty := false
	var meta PagedArrayMeta

	if errors.Is(err, os.ErrNotExist) {
		meta = PagedArrayMeta{
			Version:     1,
			Length:      0,
			ElementSize: elementSize,
		}

		dirty = true
	} else if err != nil {
		return nil, err
	} else {
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return nil, err
		}

		if meta.Version != 1 {
			return nil, fmt.Errorf("unsupported PagedArray version: %d", meta.Version)
		}

		if meta.ElementSize != elementSize {
			return nil, fmt.Errorf("element size mismatch: file has %d bytes, type has %d bytes", meta.ElementSize, elementSize)
		}
	}

	array := &PagedArray[T]{
		directory:       directory,
		files:           make(map[uint64]*os.File),
		elementSize:     elementSize,
		elementsPerPage: elementsPerPage,
		pagesPerSegment: pagesPerSegment,
		length:          meta.Length,
		dirty:           dirty,
	}

	cache, err := lru.NewWithEvict[uint64, *Page](maxLoadedPages, func(key uint64, page *Page) {
		if page.dirty {
			_ = array.writePage(page)
		}
	})
	if err != nil {
		return nil, err
	}

	array.cache = cache
	return array, nil
}

func (a *PagedArray[T]) Len() int {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.length
}

func (a *PagedArray[T]) At(index int) (T, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	var zero T

	idx := uint64(index)
	globalPageKey := idx / a.elementsPerPage
	elementOffset := idx % a.elementsPerPage
	byteOffset := elementOffset * a.elementSize

	page, err := a.getPage(globalPageKey)
	if err != nil {
		return zero, err
	}

	var value T
	src := page.data[byteOffset : byteOffset+a.elementSize]
	dst := (*[1 << 24]byte)(unsafe.Pointer(&value))[:a.elementSize:a.elementSize]
	copy(dst, src)

	return value, nil
}

func (a *PagedArray[T]) Set(index int, value T) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	idx := uint64(index)
	globalPageKey := idx / a.elementsPerPage
	elementOffset := idx % a.elementsPerPage
	byteOffset := elementOffset * a.elementSize

	page, err := a.getPage(globalPageKey)
	if err != nil {
		return err
	}

	src := (*[1 << 24]byte)(unsafe.Pointer(&value))[:a.elementSize:a.elementSize]
	dst := page.data[byteOffset : byteOffset+a.elementSize]
	copy(dst, src)

	page.dirty = true

	if index >= a.length {
		a.length = index + 1
	}

	a.dirty = true

	return nil
}

func (a *PagedArray[T]) Slice(start int, end int, dest []T) ([]T, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	count := end - start
	if count <= 0 {
		if dest == nil {
			return []T{}, nil
		}

		return dest[:0], nil
	}

	if cap(dest) < count {
		dest = make([]T, count)
	}

	dest = dest[:count]

	// Copy data page by page
	elementsCopied := 0

	for elementsCopied < count {
		currentIndex := uint64(start + elementsCopied)
		globalPageKey := currentIndex / a.elementsPerPage
		elementOffset := currentIndex % a.elementsPerPage

		page, err := a.getPage(globalPageKey)
		if err != nil {
			return nil, err
		}

		elementsToCopyFromPage := a.elementsPerPage - elementOffset
		if elementsCopied+int(elementsToCopyFromPage) > count {
			elementsToCopyFromPage = uint64(count - elementsCopied)
		}

		byteOffset := elementOffset * a.elementSize
		byteLength := elementsToCopyFromPage * a.elementSize

		src := page.data[byteOffset : byteOffset+byteLength]

		// Use unsafe pointer to copy directly into dest slice
		dstPtr := unsafe.Pointer(&dest[elementsCopied])
		dst := (*[1 << 30]byte)(dstPtr)[:byteLength:byteLength]
		copy(dst, src)

		elementsCopied += int(elementsToCopyFromPage)
	}

	return dest, nil
}

func (a *PagedArray[T]) Flush() error {
	a.lock.Lock()
	defer a.lock.Unlock()

	if !a.dirty {
		return nil
	}

	var err error

	keys := a.cache.Keys()

	for _, key := range keys {
		if page, ok := a.cache.Peek(key); ok {
			if page.dirty {
				if writeError := a.writePage(page); writeError != nil {
					err = errors.Join(err, writeError)
				} else {
					page.dirty = false
				}
			}
		}
	}

	meta := PagedArrayMeta{
		Version:     1,
		Length:      a.length,
		ElementSize: a.elementSize,
	}

	metaBytes, writeError := json.Marshal(meta)
	if writeError != nil {
		err = errors.Join(err, writeError)
	} else {
		metaPath := filepath.Join(a.directory, "meta.json")

		if writeError := os.WriteFile(metaPath, metaBytes, 0644); writeError != nil {
			err = errors.Join(err, writeError)
		}
	}

	a.dirty = false
	return err
}

func (a *PagedArray[T]) Close() error {
	// Don't lock yet because Flush locks
	var err error

	if flushErr := a.Flush(); flushErr != nil {
		err = errors.Join(err, flushErr)
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	a.cache.Purge() // Automatically triggers OnEvict for all loaded pages

	for _, file := range a.files {
		err = errors.Join(err, file.Close())
	}

	return err
}

func (a *PagedArray[T]) getPage(globalPageKey uint64) (*Page, error) {
	if page, exists := a.cache.Get(globalPageKey); exists {
		return page, nil
	}

	data, err := a.readPage(globalPageKey)
	if err != nil {
		return nil, err
	}

	page := &Page{key: globalPageKey, data: data}
	a.cache.Add(globalPageKey, page)

	return page, nil
}

func (a *PagedArray[T]) getFile(segmentIdx uint64) (*os.File, error) {
	if file, exists := a.files[segmentIdx]; exists {
		return file, nil
	}

	path := filepath.Join(a.directory, fmt.Sprintf("segment_%05d.bin", segmentIdx))

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	a.files[segmentIdx] = file

	return file, nil
}

func (a *PagedArray[T]) readPage(key uint64) ([]byte, error) {
	segmentIdx := key / a.pagesPerSegment
	pageIdx := key % a.pagesPerSegment

	file, err := a.getFile(segmentIdx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, PageSize)
	offset := int64(pageIdx * PageSize)

	size, err := file.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if size < PageSize {
		for i := size; i < PageSize; i++ {
			buf[i] = 0
		}
	}

	return buf, nil
}

func (a *PagedArray[T]) writePage(page *Page) error {
	segmentIdx := page.key / a.pagesPerSegment
	pageIdx := page.key % a.pagesPerSegment

	file, err := a.getFile(segmentIdx)
	if err != nil {
		return err
	}

	offset := int64(pageIdx * PageSize)
	_, err = file.WriteAt(page.data, offset)

	return err
}
