package collections

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"unsafe"
)

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

	file     *os.File
	readOnly bool

	elemCount    int
	elemSize     int
	pageSize     int // bytes per page
	elemsPerPage int
	dataOffset   int64 // byte offset of element 0 within the file

	maxPages int // hard cap on resident pages

	pages map[int]*list.Element // pageNo -> *list element holding *residentPage
	lru   *list.List            // most-recently-used at front
	pool  [][]byte              // recycled page buffers
}

type residentPage struct {
	no    int
	buf   []byte
	dirty bool
}

const (
	pagedArrayMagic   = "PAGEDARR"
	pagedArrayVersion = uint32(1)

	// headerSize is the fixed on-disk header size in bytes. Element 0 begins at
	// the first page-aligned offset at or after headerSize.
	pagedArrayHeaderSize = 64

	// DefaultPageSize is the default page size in bytes used by
	// CreatePagedArrayFile when a non-positive page size is supplied.
	DefaultPageSize = 64 * 1024
)

// CreatePagedArrayFile writes data to a new paged-array file at path using the
// given page size in bytes. If pageSizeBytes is non-positive, DefaultPageSize
// is used. The element type T must be pointer-free (see PagedArray).
func CreatePagedArrayFile[T any](path string, data []T, pageSizeBytes int) (err error) {
	var zero T

	elemSize := int(unsafe.Sizeof(zero))
	if elemSize == 0 {
		return fmt.Errorf("paged array: zero-sized element type is not supported")
	}

	if pageSizeBytes <= 0 {
		pageSizeBytes = DefaultPageSize
	}

	if pageSizeBytes < elemSize {
		return fmt.Errorf("paged array: page size %d is smaller than element size %d", pageSizeBytes, elemSize)
	}

	elemsPerPage := pageSizeBytes / elemSize
	dataOffset := pageAlign(pagedArrayHeaderSize, pageSizeBytes)

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	header := make([]byte, dataOffset)
	copy(header[0:8], pagedArrayMagic)
	binary.LittleEndian.PutUint32(header[8:12], pagedArrayVersion)
	binary.LittleEndian.PutUint64(header[12:20], uint64(elemSize))
	binary.LittleEndian.PutUint64(header[20:28], uint64(len(data)))
	binary.LittleEndian.PutUint64(header[28:36], uint64(pageSizeBytes))

	if _, err = file.Write(header); err != nil {
		return err
	}

	// Write elements one page at a time so we never materialize a second full
	// copy of the data in memory.
	for start := 0; start < len(data); start += elemsPerPage {
		end := start + elemsPerPage
		if end > len(data) {
			end = len(data)
		}

		if _, err = file.Write(elemsToBytes(data[start:end])); err != nil {
			return err
		}
	}

	return nil
}

// OpenPagedArray opens an existing paged-array file at path for reading. The
// budgetBytes argument bounds the resident memory used by the page cache; the
// number of resident pages is at most max(1, budgetBytes/pageSize). The element
// type T must match the type the file was created with.
func OpenPagedArray[T any](path string, budgetBytes int) (*PagedArray[T], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	array, err := newPagedArrayFromFile[T](file, budgetBytes, true)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return array, nil
}

func createWritablePagedArray[T any](path string, elemCount int, pageSizeBytes int, budgetBytes int) (*PagedArray[T], error) {
	var zero T

	elemSize := int(unsafe.Sizeof(zero))
	if elemSize == 0 {
		return nil, fmt.Errorf("paged array: zero-sized element type is not supported")
	}

	if pageSizeBytes <= 0 {
		pageSizeBytes = DefaultPageSize
	}
	if pageSizeBytes < elemSize {
		return nil, fmt.Errorf("paged array: page size %d is smaller than element size %d", pageSizeBytes, elemSize)
	}

	elemsPerPage := pageSizeBytes / elemSize
	dataOffset := pageAlign(pagedArrayHeaderSize, pageSizeBytes)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}

	header := make([]byte, dataOffset)
	copy(header[0:8], pagedArrayMagic)
	binary.LittleEndian.PutUint32(header[8:12], pagedArrayVersion)
	binary.LittleEndian.PutUint64(header[12:20], uint64(elemSize))
	binary.LittleEndian.PutUint64(header[20:28], uint64(elemCount))
	binary.LittleEndian.PutUint64(header[28:36], uint64(pageSizeBytes))
	if _, err = f.Write(header); err != nil {
		_ = f.Close()
		return nil, err
	}

	// Pre-size the file so WriteAt into any page offset is valid.
	pageCount := (elemCount + elemsPerPage - 1) / elemsPerPage
	totalSize := dataOffset + int64(pageCount)*int64(pageSizeBytes)
	if err = f.Truncate(totalSize); err != nil {
		_ = f.Close()
		return nil, err
	}

	pa, err := newPagedArrayFromFile[T](f, budgetBytes, false)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return pa, nil
}

func newPagedArrayFromFile[T any](f *os.File, budgetBytes int, readOnly bool) (*PagedArray[T], error) {
	var zero T
	wantElemSize := int(unsafe.Sizeof(zero))

	header := make([]byte, pagedArrayHeaderSize)
	if _, err := f.ReadAt(header, 0); err != nil {
		return nil, fmt.Errorf("paged array: reading header: %w", err)
	}

	if string(header[0:8]) != pagedArrayMagic {
		return nil, fmt.Errorf("paged array: bad magic")
	}
	if v := binary.LittleEndian.Uint32(header[8:12]); v != pagedArrayVersion {
		return nil, fmt.Errorf("paged array: unsupported version %d", v)
	}
	elemSize := int(binary.LittleEndian.Uint64(header[12:20]))
	if elemSize != wantElemSize {
		return nil, fmt.Errorf("paged array: element size mismatch: file has %d, type T is %d", elemSize, wantElemSize)
	}
	elemCount := int(binary.LittleEndian.Uint64(header[20:28]))
	pageSize := int(binary.LittleEndian.Uint64(header[28:36]))
	if pageSize < elemSize {
		return nil, fmt.Errorf("paged array: invalid page size %d", pageSize)
	}

	maxPages := budgetBytes / pageSize
	if maxPages < 1 {
		maxPages = 1
	}

	return &PagedArray[T]{
		file:         f,
		readOnly:     readOnly,
		elemCount:    elemCount,
		elemSize:     elemSize,
		pageSize:     pageSize,
		elemsPerPage: pageSize / elemSize,
		dataOffset:   pageAlign(pagedArrayHeaderSize, pageSize),
		maxPages:     maxPages,
		pages:        make(map[int]*list.Element),
		lru:          list.New(),
	}, nil
}

// Len returns the number of elements in the array.
func (a *PagedArray[T]) Len() int {
	return a.elemCount
}

// At returns the element at index i. The element is copied out of the resident
// page, so it remains valid after the page is evicted. It panics if i is out of
// range.
func (a *PagedArray[T]) At(i int) T {
	if i < 0 || i >= a.elemCount {
		panic(fmt.Sprintf("paged array: index %d out of range [0,%d)", i, a.elemCount))
	}

	pageNo := i / a.elemsPerPage
	off := i % a.elemsPerPage

	a.lock.Lock()
	defer a.lock.Unlock()

	rp := a.getPageLocked(pageNo)
	return elementAt[T](rp.buf, off)
}

// SetAt writes v to index i. It is only valid on writable arrays (created via
// createWritablePagedArray). The page is marked dirty and flushed to disk on
// eviction or Flush. It panics if i is out of range.
func (a *PagedArray[T]) SetAt(i int, v T) error {
	if a.readOnly {
		return fmt.Errorf("paged array: SetAt on read-only array")
	}
	if i < 0 || i >= a.elemCount {
		panic(fmt.Sprintf("paged array: index %d out of range [0,%d)", i, a.elemCount))
	}

	pageNo := i / a.elemsPerPage
	off := i % a.elemsPerPage

	a.lock.Lock()
	defer a.lock.Unlock()

	rp := a.getPageLocked(pageNo)
	setElementAt[T](rp.buf, off, v)
	rp.dirty = true
	return nil
}

// Slice copies the half-open range [start, end) into dst and returns the filled
// sub-slice. dst is grown if its capacity is insufficient. The data is always
// copied (never a view into page buffers), so the result is stable across
// subsequent evictions. It panics if the range is invalid.
func (a *PagedArray[T]) Slice(start, end int, dst []T) []T {
	if start < 0 || end > a.elemCount || start > end {
		panic(fmt.Sprintf("paged array: invalid range [%d,%d) for length %d", start, end, a.elemCount))
	}

	n := end - start
	if cap(dst) < n {
		dst = make([]T, n)
	}
	dst = dst[:n]
	if n == 0 {
		return dst
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	written := 0
	for idx := start; idx < end; {
		pageNo := idx / a.elemsPerPage
		off := idx % a.elemsPerPage

		// Number of elements available from idx to the end of this page.
		avail := a.elemsPerPage - off
		remaining := end - idx
		take := avail
		if remaining < take {
			take = remaining
		}

		rp := a.getPageLocked(pageNo)
		src := elementsSlice[T](rp.buf, off, take)
		copy(dst[written:written+take], src)

		written += take
		idx += take
	}

	return dst
}

// Flush writes all dirty resident pages to disk. It is a no-op on read-only
// arrays.
func (a *PagedArray[T]) Flush() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.flushLocked()
}

func (a *PagedArray[T]) flushLocked() error {
	if a.readOnly {
		return nil
	}
	for el := a.lru.Back(); el != nil; el = el.Prev() {
		rp := el.Value.(*residentPage)
		if rp.dirty {
			if err := a.writePageLocked(rp); err != nil {
				return err
			}
			rp.dirty = false
		}
	}
	return nil
}

// Close flushes any dirty pages and closes the underlying file.
func (a *PagedArray[T]) Close() error {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.file == nil {
		return nil
	}
	if err := a.flushLocked(); err != nil {
		_ = a.file.Close()
		a.file = nil
		return err
	}
	err := a.file.Close()
	a.file = nil
	return err
}

// getPageLocked returns the resident page for pageNo, faulting it in (and
// evicting as needed) if not already resident. Callers must hold a.lock.
func (a *PagedArray[T]) getPageLocked(pageNo int) *residentPage {
	if el, ok := a.pages[pageNo]; ok {
		a.lru.MoveToFront(el)
		return el.Value.(*residentPage)
	}

	// Evict until there is room for one more page.
	for len(a.pages) >= a.maxPages {
		a.evictLocked()
	}

	buf := a.acquireBuffer()
	a.readPageInto(pageNo, buf)

	rp := &residentPage{no: pageNo, buf: buf}
	el := a.lru.PushFront(rp)
	a.pages[pageNo] = el
	return rp
}

// evictLocked removes the least-recently-used page, flushing it first if dirty,
// and recycles its buffer. Callers must hold a.lock.
func (a *PagedArray[T]) evictLocked() {
	el := a.lru.Back()
	if el == nil {
		return
	}
	rp := el.Value.(*residentPage)

	if rp.dirty {
		// Best-effort flush on eviction; surfaced again via Flush/Close.
		_ = a.writePageLocked(rp)
		rp.dirty = false
	}

	a.lru.Remove(el)
	delete(a.pages, rp.no)
	a.pool = append(a.pool, rp.buf)
}

func (a *PagedArray[T]) acquireBuffer() []byte {
	if n := len(a.pool); n > 0 {
		buf := a.pool[n-1]
		a.pool = a.pool[:n-1]
		return buf
	}
	return make([]byte, a.pageSize)
}

// readPageInto reads page pageNo from disk into buf. Bytes beyond the end of the
// file (for a partial final page) are zeroed.
func (a *PagedArray[T]) readPageInto(pageNo int, buf []byte) {
	offset := a.dataOffset + int64(pageNo)*int64(a.pageSize)
	n, err := a.file.ReadAt(buf, offset)
	if err == io.EOF || (err == nil && n < len(buf)) {
		// Partial final page: zero the remainder.
		for i := n; i < len(buf); i++ {
			buf[i] = 0
		}
		return
	}
	if err != nil && err != io.EOF {
		panic(fmt.Sprintf("paged array: read page %d: %v", pageNo, err))
	}
}

func (a *PagedArray[T]) writePageLocked(rp *residentPage) error {
	offset := a.dataOffset + int64(rp.no)*int64(a.pageSize)
	_, err := a.file.WriteAt(rp.buf, offset)
	return err
}

// pageAlign returns the smallest multiple of pageSize that is >= n.
func pageAlign(n int64, pageSize int) int64 {
	ps := int64(pageSize)
	return ((n + ps - 1) / ps) * ps
}

// --- raw element <-> bytes helpers (pointer-free T only) ---

// elemsToBytes reinterprets a []T as a []byte over the same memory.
func elemsToBytes[T any](elems []T) []byte {
	if len(elems) == 0 {
		return nil
	}
	elemSize := int(unsafe.Sizeof(elems[0]))
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(elems))), len(elems)*elemSize)
}

// elementAt reads the element at element-index off from a raw page buffer.
func elementAt[T any](buf []byte, off int) T {
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	base := off * elemSize
	return *(*T)(unsafe.Pointer(&buf[base]))
}

// setElementAt writes v at element-index off into a raw page buffer.
func setElementAt[T any](buf []byte, off int, v T) {
	elemSize := int(unsafe.Sizeof(v))
	base := off * elemSize
	*(*T)(unsafe.Pointer(&buf[base])) = v
}

// elementsSlice returns a []T view over count elements starting at element-index
// off within a raw page buffer. The view aliases buf and must be consumed before
// buf is reused.
func elementsSlice[T any](buf []byte, off, count int) []T {
	var zero T
	elemSize := int(unsafe.Sizeof(zero))
	base := off * elemSize
	return unsafe.Slice((*T)(unsafe.Pointer(&buf[base])), count)
}
