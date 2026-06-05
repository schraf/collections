package collections

// BiCSR holds two CSRs over a shared node-id space: an outbound adjacency
// (keyed on edge source) and an inbound adjacency (keyed on edge target). It is
// the typical shape for graph traversal, where a hop may follow edges in either
// direction.
//
// Both CSRs are built from the same edge stream via BiCSRBuilder, so a given
// external node id maps to the same dense index in both. This lets a traversal
// carry dense indices across outbound and inbound lookups without re-resolving.
type BiCSR[N comparable, L comparable] struct {
	out *CSR[N, L]
	in  *CSR[N, L]
}

// Out returns the outbound adjacency (neighbors are edge targets).
func (b *BiCSR[N, L]) Out() *CSR[N, L] { return b.out }

// In returns the inbound adjacency (neighbors are edge sources).
func (b *BiCSR[N, L]) In() *CSR[N, L] { return b.in }

// NumNodes returns the shared node count.
func (b *BiCSR[N, L]) NumNodes() int { return b.out.NumNodes() }

// NumEdges returns the number of directed edges (equal in each direction).
func (b *BiCSR[N, L]) NumEdges() int { return b.out.NumEdges() }

// Dense resolves an external node id to its shared dense index.
func (b *BiCSR[N, L]) Dense(id N) (int32, bool) { return b.out.Dense(id) }

// NodeId resolves a shared dense index to its external node id.
func (b *BiCSR[N, L]) NodeId(dense int32) N { return b.out.NodeId(dense) }

// BiCSRBuilder accumulates edges once and produces a BiCSR whose outbound and
// inbound CSRs share a single dense-index and label space. Not safe for
// concurrent use.
type BiCSRBuilder[N comparable, L comparable] struct {
	denseToId []N
	idToDense map[N]int32

	labelToCode map[L]uint8
	codeToLabel []L

	srcs   []int32
	dsts   []int32
	labels []uint8

	err error
}

// NewBiCSRBuilder returns an empty BiCSRBuilder pre-sized by the hints.
func NewBiCSRBuilder[N comparable, L comparable](nodeHint, edgeHint int) *BiCSRBuilder[N, L] {
	if nodeHint < 0 {
		nodeHint = 0
	}
	if edgeHint < 0 {
		edgeHint = 0
	}

	return &BiCSRBuilder[N, L]{
		denseToId:   make([]N, 0, nodeHint),
		idToDense:   make(map[N]int32, nodeHint),
		labelToCode: make(map[L]uint8),
		srcs:        make([]int32, 0, edgeHint),
		dsts:        make([]int32, 0, edgeHint),
		labels:      make([]uint8, 0, edgeHint),
	}
}

// AddNode ensures id has a dense index, returning it.
func (b *BiCSRBuilder[N, L]) AddNode(id N) int32 { return b.dense(id) }

// AddEdge records a directed edge src -> dst with the given label. Errors (e.g.
// exceeding MaxCSRLabels) are recorded and surfaced by Build.
func (b *BiCSRBuilder[N, L]) AddEdge(src, dst N, label L) {
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
func (b *BiCSRBuilder[N, L]) NumNodes() int { return len(b.denseToId) }

// NumEdges returns the number of edges accumulated so far.
func (b *BiCSRBuilder[N, L]) NumEdges() int { return len(b.srcs) }

func (b *BiCSRBuilder[N, L]) dense(id N) int32 {
	if d, ok := b.idToDense[id]; ok {
		return d
	}
	d := int32(len(b.denseToId))
	b.idToDense[id] = d
	b.denseToId = append(b.denseToId, id)
	return d
}

// Build produces the BiCSR. Both directions reference the same dense-index and
// label tables (shared, read-only). The builder must not be reused afterward.
func (b *BiCSRBuilder[N, L]) Build() (*BiCSR[N, L], error) {
	if b.err != nil {
		return nil, b.err
	}

	bi, _, _ := b.build()
	return bi, nil
}

// BuildBiCSRWithData builds the BiCSR and reorders a caller-supplied per-edge
// payload slice (in AddEdge order, len == builder.NumEdges()) into both the
// outbound and inbound CSR flat-position orders. The returned outData is
// parallel to the outbound CSR's neighbors/labels; inData to the inbound CSR's.
// Look them up via the CSR's EdgeRange/Edges. This lets callers attach per-edge
// data (e.g. a database id) without a separate map, while keeping the builder
// and CSR types free of a payload type parameter.
//
// The builder is consumed (as with Build) and must not be reused.
func BuildBiCSRWithData[N comparable, L comparable, D any](b *BiCSRBuilder[N, L], data []D) (bi *BiCSR[N, L], outData []D, inData []D, err error) {
	if b.err != nil {
		return nil, nil, nil, b.err
	}

	if len(data) != len(b.srcs) {
		return nil, nil, nil, errCSRDataLength(len(data), len(b.srcs))
	}

	bi, outPerm, inPerm := b.build()

	outData = PermuteCSRData(data, outPerm)
	inData = PermuteCSRData(data, inPerm)

	return bi, outData, inData, nil
}

// build is the shared core of Build/BuildBiCSRWithData. It produces the BiCSR
// and the per-direction accumulation→position permutations (outPerm aligns with
// the outbound CSR's flat positions; inPerm with the inbound CSR's).
func (b *BiCSRBuilder[N, L]) build() (bi *BiCSR[N, L], outPerm []int32, inPerm []int32) {
	numNodes := len(b.denseToId)

	outLayout := fillCSR(numNodes, b.srcs, b.dsts, b.labels)
	inLayout := fillCSR(numNodes, b.dsts, b.srcs, b.labels)

	out := &CSR[N, L]{
		offsets:     outLayout.offsets,
		neighbors:   outLayout.neighbors,
		labels:      outLayout.labels,
		denseToId:   b.denseToId,
		idToDense:   b.idToDense,
		labelToCode: b.labelToCode,
		codeToLabel: b.codeToLabel,
	}
	in := &CSR[N, L]{
		offsets:     inLayout.offsets,
		neighbors:   inLayout.neighbors,
		labels:      inLayout.labels,
		denseToId:   b.denseToId,
		idToDense:   b.idToDense,
		labelToCode: b.labelToCode,
		codeToLabel: b.codeToLabel,
	}

	b.srcs = nil
	b.dsts = nil
	b.labels = nil
	b.denseToId = nil
	b.idToDense = nil
	b.labelToCode = nil
	b.codeToLabel = nil

	return &BiCSR[N, L]{out: out, in: in}, outLayout.perm, inLayout.perm
}
