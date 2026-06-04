package collections

import "fmt"

// CSRBuilder accumulates edges and produces an immutable CSR. It is not safe
// for concurrent use; build from a single goroutine (typically a streaming
// load).
//
// Nodes are assigned dense indices lazily in first-seen order as they appear as
// an edge endpoint or via AddNode. Edge labels are interned to a one-byte code
// in first-seen order.
type CSRBuilder[N comparable, L comparable] struct {
	denseToID []N
	idToDense map[N]int32

	labelToCode map[L]uint8
	codeToLabel []L

	// edges accumulated as dense (src,dst,labelCode) triples.
	srcs   []int32
	dsts   []int32
	labels []uint8

	err error
}

// errTooManyCSRLabels is returned when a builder exceeds the one-byte label
// space.
func errTooManyCSRLabels() error {
	return fmt.Errorf("collections: csr has too many distinct labels (max %d)", MaxCSRLabels)
}

// NewCSRBuilder returns an empty CSRBuilder. nodeHint and edgeHint pre-size
// internal buffers for the expected node and edge counts (0 is fine).
func NewCSRBuilder[N comparable, L comparable](nodeHint, edgeHint int) *CSRBuilder[N, L] {
	if nodeHint < 0 {
		nodeHint = 0
	}
	if edgeHint < 0 {
		edgeHint = 0
	}

	return &CSRBuilder[N, L]{
		denseToID:   make([]N, 0, nodeHint),
		idToDense:   make(map[N]int32, nodeHint),
		labelToCode: make(map[L]uint8),
		srcs:        make([]int32, 0, edgeHint),
		dsts:        make([]int32, 0, edgeHint),
		labels:      make([]uint8, 0, edgeHint),
	}
}

// AddNode ensures id has a dense index, returning it. Useful for registering
// isolated nodes (no edges) so they still appear in the CSR.
func (b *CSRBuilder[N, L]) AddNode(id N) int32 {
	return b.dense(id)
}

// AddEdge records a directed edge src -> dst with the given label. Endpoints
// are registered as nodes if not seen before. Errors (e.g. exceeding
// MaxCSRLabels) are recorded and surfaced by Build.
func (b *CSRBuilder[N, L]) AddEdge(src, dst N, label L) {
	if b.err != nil {
		return
	}

	code, ok := b.labelToCode[label]
	if !ok {
		if len(b.codeToLabel) >= MaxCSRLabels {
			b.err = errTooManyCSRLabels()
			return
		}
		code = uint8(len(b.codeToLabel))
		b.labelToCode[label] = code
		b.codeToLabel = append(b.codeToLabel, label)
	}

	b.srcs = append(b.srcs, b.dense(src))
	b.dsts = append(b.dsts, b.dense(dst))
	b.labels = append(b.labels, code)
}

// NumNodes returns the number of distinct nodes seen so far.
func (b *CSRBuilder[N, L]) NumNodes() int { return len(b.denseToID) }

// NumEdges returns the number of edges accumulated so far.
func (b *CSRBuilder[N, L]) NumEdges() int { return len(b.srcs) }

func (b *CSRBuilder[N, L]) dense(id N) int32 {
	if d, ok := b.idToDense[id]; ok {
		return d
	}
	d := int32(len(b.denseToID))
	b.idToDense[id] = d
	b.denseToID = append(b.denseToID, id)
	return d
}

// Build produces the immutable CSR. The builder's accumulated edge buffers are
// released so the resulting CSR holds only the compact final arrays. After
// Build the builder must not be reused.
func (b *CSRBuilder[N, L]) Build() (*CSR[N, L], error) {
	if b.err != nil {
		return nil, b.err
	}

	offsets, neighbors, labels := fillCSR(len(b.denseToID), b.srcs, b.dsts, b.labels)

	c := &CSR[N, L]{
		offsets:     offsets,
		neighbors:   neighbors,
		labels:      labels,
		denseToID:   b.denseToID,
		idToDense:   b.idToDense,
		labelToCode: b.labelToCode,
		codeToLabel: b.codeToLabel,
	}

	// Release accumulation buffers; ownership of the identity/label tables
	// transfers to the CSR.
	b.srcs = nil
	b.dsts = nil
	b.labels = nil
	b.denseToID = nil
	b.idToDense = nil
	b.labelToCode = nil
	b.codeToLabel = nil

	return c, nil
}
