package collections

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unsafe"
)

// StreamingGraphBuilder constructs a CSR DirectedGraph backed by disk-resident
// PagedArrays without ever holding the full edge set in RAM. Edges are appended
// to an on-disk scratch stream as they arrive; the final CSR is produced by a
// two-pass counting sort that scatters edges (and labels) directly into
// pre-sized, writable paged-array files.
//
// The offsets and a per-node scatter cursor are kept in memory. These are
// proportional to the node count (not the edge count), which is the dominant
// term for very large graphs; edge data is what is streamed to disk.
//
// Element type constraint: as with PagedArray, EdgeType must be a pointer-free,
// fixed-size value type.
//
// A StreamingGraphBuilder owns a working directory of scratch and output files.
// Call Close to release file handles. The DirectedGraphs returned by the build
// methods reference paged-array files within dir and remain valid until those
// files are removed.
type StreamingGraphBuilder[OffsetType Unsigned, EdgeType any] struct {
	dir         string
	nodeCount   DenseId
	withLabels  bool
	budgetBytes int
	pageSize    int
	labelSize   int

	edgeCount   int
	scratchPath string
	scratch     *os.File
	scratchBuf  *bufio.Writer

	recordSize int
	closed     bool
}

// NewStreamingGraphBuilder creates a streaming builder that writes scratch and
// output files under dir (which must already exist). budgetBytes bounds the
// resident memory of each paged array used during the build. pageSizeBytes may
// be 0 to use DefaultPageSize.
func NewStreamingGraphBuilder[OffsetType Unsigned, EdgeType any](dir string, nodeCount DenseId, withLabels bool, budgetBytes int, pageSizeBytes int) (*StreamingGraphBuilder[OffsetType, EdgeType], error) {
	if pageSizeBytes <= 0 {
		pageSizeBytes = DefaultPageSize
	}

	var label EdgeType
	labelSize := int(unsafe.Sizeof(label))

	// Scratch record layout: from(uint32) + to(uint32) [+ label(labelSize)].
	recordSize := 8
	if withLabels {
		recordSize += labelSize
	}

	scratchPath := filepath.Join(dir, "edges.scratch")
	f, err := os.Create(scratchPath)
	if err != nil {
		return nil, err
	}

	return &StreamingGraphBuilder[OffsetType, EdgeType]{
		dir:         dir,
		nodeCount:   nodeCount,
		withLabels:  withLabels,
		budgetBytes: budgetBytes,
		pageSize:    pageSizeBytes,
		labelSize:   labelSize,
		scratchPath: scratchPath,
		scratch:     f,
		scratchBuf:  bufio.NewWriterSize(f, 1<<20),
		recordSize:  recordSize,
	}, nil
}

// AddEdgeWithLabel appends a labeled edge to the scratch stream.
func (b *StreamingGraphBuilder[OffsetType, EdgeType]) AddEdgeWithLabel(from DenseId, to DenseId, label EdgeType) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], from)
	binary.LittleEndian.PutUint32(hdr[4:8], to)
	if _, err := b.scratchBuf.Write(hdr[:]); err != nil {
		return err
	}

	if b.withLabels && b.labelSize > 0 {
		if _, err := b.scratchBuf.Write(labelBytesScratch(&label, b.labelSize)); err != nil {
			return err
		}
	}

	b.edgeCount++
	return nil
}

// AddEdge appends an unlabeled edge (zero label when the graph is labeled).
func (b *StreamingGraphBuilder[OffsetType, EdgeType]) AddEdge(from DenseId, to DenseId) error {
	var zero EdgeType
	return b.AddEdgeWithLabel(from, to, zero)
}

// BuildOutboundGraph builds the CSR keyed on edge source (neighbor is the edge
// target).
func (b *StreamingGraphBuilder[OffsetType, EdgeType]) BuildOutboundGraph() (*DirectedGraph[OffsetType, EdgeType], error) {
	return b.build("outbound", false)
}

// BuildInboundGraph builds the CSR keyed on edge target (neighbor is the edge
// source).
func (b *StreamingGraphBuilder[OffsetType, EdgeType]) BuildInboundGraph() (*DirectedGraph[OffsetType, EdgeType], error) {
	return b.build("inbound", true)
}

// build performs the two-pass counting sort. When inbound is true, the key is
// the edge target and the neighbor is the edge source; otherwise the key is the
// source and the neighbor is the target.
func (b *StreamingGraphBuilder[OffsetType, EdgeType]) build(name string, inbound bool) (*DirectedGraph[OffsetType, EdgeType], error) {
	if err := b.scratchBuf.Flush(); err != nil {
		return nil, err
	}

	// Pass A: degree count + prefix sum (in memory, sized by node count).
	offsets := make([]OffsetType, b.nodeCount+1)
	if err := b.scanScratch(func(from, to DenseId, _ []byte) {
		key := from
		if inbound {
			key = to
		}
		offsets[key+1]++
	}); err != nil {
		return nil, err
	}
	for i := DenseId(0); i < b.nodeCount; i++ {
		offsets[i+1] += offsets[i]
	}

	// Create the writable output paged arrays, pre-sized to edgeCount.
	edgesPath := filepath.Join(b.dir, name+".edges")
	edges, err := createWritablePagedArray[DenseId](edgesPath, b.edgeCount, b.pageSize, b.budgetBytes)
	if err != nil {
		return nil, err
	}

	var labels *PagedArray[EdgeType]
	if b.withLabels {
		labelsPath := filepath.Join(b.dir, name+".labels")
		labels, err = createWritablePagedArray[EdgeType](labelsPath, b.edgeCount, b.pageSize, b.budgetBytes)
		if err != nil {
			_ = edges.Close()
			return nil, err
		}
	}

	// Pass B: scatter into final positions.
	cursor := make([]OffsetType, b.nodeCount)
	copy(cursor, offsets[:b.nodeCount])

	scatterErr := b.scanScratch(func(from, to DenseId, labelBytes []byte) {
		key := from
		neighbor := to
		if inbound {
			key = to
			neighbor = from
		}
		pos := int(cursor[key])
		if err := edges.SetAt(pos, neighbor); err != nil {
			panic(err)
		}
		if b.withLabels {
			label := decodeLabel[EdgeType](labelBytes, b.labelSize)
			if err := labels.SetAt(pos, label); err != nil {
				panic(err)
			}
		}
		cursor[key]++
	})
	if scatterErr != nil {
		_ = edges.Close()
		if labels != nil {
			_ = labels.Close()
		}
		return nil, scatterErr
	}

	// Flush dirty pages and reopen the outputs read-only for traversal.
	if err := edges.Close(); err != nil {
		return nil, err
	}
	if labels != nil {
		if err := labels.Close(); err != nil {
			return nil, err
		}
	}

	edgesRO, err := OpenPagedArray[DenseId](edgesPath, b.budgetBytes)
	if err != nil {
		return nil, err
	}

	var labelsArray Array[EdgeType]
	if b.withLabels {
		labelsRO, err := OpenPagedArray[EdgeType](filepath.Join(b.dir, name+".labels"), b.budgetBytes)
		if err != nil {
			_ = edgesRO.Close()
			return nil, err
		}
		labelsArray = labelsRO
	}

	return NewDirectedGraphFromArrays[OffsetType, EdgeType](
		NewSliceArray(offsets),
		edgesRO,
		labelsArray,
	), nil
}

// scanScratch streams every record in the scratch file, invoking fn for each.
// The labelBytes passed to fn aliases an internal buffer and is only valid for
// the duration of the call.
func (b *StreamingGraphBuilder[OffsetType, EdgeType]) scanScratch(fn func(from, to DenseId, labelBytes []byte)) error {
	if _, err := b.scratch.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReaderSize(b.scratch, 1<<20)

	rec := make([]byte, b.recordSize)
	for {
		_, err := io.ReadFull(r, rec)
		if err == io.EOF {
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			return fmt.Errorf("streaming builder: truncated scratch record")
		}
		if err != nil {
			return err
		}

		from := binary.LittleEndian.Uint32(rec[0:4])
		to := binary.LittleEndian.Uint32(rec[4:8])
		var labelBytes []byte
		if b.withLabels {
			labelBytes = rec[8:]
		}
		fn(from, to, labelBytes)
	}
}

// Close removes the scratch file and releases the scratch handle. Output
// paged-array files are left in place for use by the returned graphs.
func (b *StreamingGraphBuilder[OffsetType, EdgeType]) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true

	var firstErr error
	if b.scratch != nil {
		if err := b.scratch.Close(); err != nil {
			firstErr = err
		}
		if err := os.Remove(b.scratchPath); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// labelBytesScratch returns a raw byte view over a single label value.
func labelBytesScratch[EdgeType any](label *EdgeType, labelSize int) []byte {
	if labelSize == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(label)), labelSize)
}

// decodeLabel reconstructs an EdgeType from its raw little-endian bytes.
func decodeLabel[EdgeType any](b []byte, labelSize int) EdgeType {
	var label EdgeType
	if labelSize == 0 || len(b) == 0 {
		return label
	}
	dst := unsafe.Slice((*byte)(unsafe.Pointer(&label)), labelSize)
	copy(dst, b)
	return label
}
