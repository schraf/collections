package collections

import (
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

	array := &PagedArray[T]{
		directory:       directory,
		files:           make(map[uint64]*os.File),
		elementSize:     elementSize,
		elementsPerPage: elementsPerPage,
		pagesPerSegment: pagesPerSegment,
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

func (a *PagedArray[T]) Get(index uint64) (T, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	globalPageKey := index / a.elementsPerPage
	elementOffset := index % a.elementsPerPage
	byteOffset := elementOffset * a.elementSize

	page, err := a.getPage(globalPageKey)
	if err != nil {
		var zero T
		return zero, err
	}

	var value T
	src := page.data[byteOffset : byteOffset+a.elementSize]
	dst := (*[1 << 24]byte)(unsafe.Pointer(&value))[:a.elementSize:a.elementSize]
	copy(dst, src)

	return value, nil
}

func (a *PagedArray[T]) Set(index uint64, value T) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	globalPageKey := index / a.elementsPerPage
	elementOffset := index % a.elementsPerPage
	byteOffset := elementOffset * a.elementSize

	page, err := a.getPage(globalPageKey)
	if err != nil {
		return err
	}

	src := (*[1 << 24]byte)(unsafe.Pointer(&value))[:a.elementSize:a.elementSize]
	dst := page.data[byteOffset : byteOffset+a.elementSize]
	copy(dst, src)

	page.dirty = true
	return nil
}

func (a *PagedArray[T]) Close() error {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.cache.Purge() // Automatically triggers OnEvict for all loaded pages

	var err error

	for _, file := range a.files {
		errors.Join(err, file.Close())
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
