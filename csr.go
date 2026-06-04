package collections

// CSR is a compact, immutable adjacency structure in compressed-sparse-row
// (CSR) form.
//
// CSR stores a directed graph's edges as three parallel arrays over dense node
// indices [0, N):
//
//	offsets   len N+1   offsets[i]..offsets[i+1] is node i's slice of edges
//	neighbors len E     destination dense index of each edge
//	labels    len E     dictionary-encoded edge label of each edge
//
// This layout keeps the whole adjacency in a handful of contiguous slices with
// no per-node or per-edge pointers, making it cache-friendly and cheap to hold
// resident for very large graphs (tens of millions of nodes / hundreds of
// millions of edges).
//
// The type is generic over the external node identifier N and the edge label L.
// Labels are interned to a uint8 code, so a CSR supports up to MaxCSRLabels
// distinct labels — ample for typed graphs while costing one byte per edge.
//
// A CSR is built once via a CSRBuilder (or BiCSRBuilder for a bidirectional
// view) and is immutable thereafter; the immutable form is safe for concurrent
// reads. To reflect mutations, build a new CSR and swap it in at a higher layer.
type CSR[N comparable, L comparable] struct {
	offsets   []int32
	neighbors []int32
	labels    []uint8

	denseToID []N
	idToDense map[N]int32

	labelToCode map[L]uint8
	codeToLabel []L
}

// MaxCSRLabels is the maximum number of distinct edge labels a CSR may hold,
// bounded by the one-byte label encoding.
const MaxCSRLabels = 256

// NumNodes returns the number of nodes.
func (c *CSR[N, L]) NumNodes() int { return len(c.denseToID) }

// NumEdges returns the number of directed edges.
func (c *CSR[N, L]) NumEdges() int { return len(c.neighbors) }

// NumLabels returns the number of distinct edge labels.
func (c *CSR[N, L]) NumLabels() int { return len(c.codeToLabel) }

// Dense resolves an external node id to its dense index. ok is false when the
// id is not present in the graph.
func (c *CSR[N, L]) Dense(id N) (dense int32, ok bool) {
	d, ok := c.idToDense[id]
	return d, ok
}

// ID resolves a dense index back to its external node id. It panics if dense is
// out of range; callers iterating known-valid indices need not check.
func (c *CSR[N, L]) ID(dense int32) N {
	return c.denseToID[dense]
}

// LabelCode resolves an external label to its internal code. ok is false when
// the label never appeared in the graph (and therefore matches no edges).
func (c *CSR[N, L]) LabelCode(label L) (code uint8, ok bool) {
	code, ok = c.labelToCode[label]
	return code, ok
}

// Label resolves an internal label code back to its external label.
func (c *CSR[N, L]) Label(code uint8) L {
	return c.codeToLabel[code]
}

// Degree returns the number of outgoing edges of the given dense node.
func (c *CSR[N, L]) Degree(dense int32) int {
	return int(c.offsets[dense+1] - c.offsets[dense])
}

// Row returns the neighbor and label-code slices for a dense node. The returned
// slices alias the CSR's internal arrays and MUST NOT be modified. They remain
// valid for the lifetime of the CSR.
func (c *CSR[N, L]) Row(dense int32) (neighbors []int32, labels []uint8) {
	start, end := c.offsets[dense], c.offsets[dense+1]
	return c.neighbors[start:end], c.labels[start:end]
}

// Neighbors returns a range-over-func iterator that yields each
// (neighborDense, labelCode) edge out of the given dense node, stopping early
// if the loop body breaks. This avoids allocating and lets callers filter by
// label cheaply.
func (c *CSR[N, L]) Neighbors(dense int32) func(yield func(neighbor int32, label uint8) bool) {
	return func(yield func(neighbor int32, label uint8) bool) {
		start, end := c.offsets[dense], c.offsets[dense+1]
		for i := start; i < end; i++ {
			if !yield(c.neighbors[i], c.labels[i]) {
				return
			}
		}
	}
}

// fillCSR builds CSR offset/neighbor/label arrays keyed on the keys slice
// (dense source indices for the direction being built), with vals as the
// neighbor dense indices and labelCodes the parallel label codes. It is
// direction-, label-, and identity-agnostic; the caller attaches the shared
// identity and label tables to the resulting CSR.
func fillCSR(numNodes int, keys, vals []int32, labelCodes []uint8) (offsets, neighbors []int32, labels []uint8) {
	offsets = make([]int32, numNodes+1)
	for _, k := range keys {
		offsets[k+1]++
	}
	for i := 0; i < numNodes; i++ {
		offsets[i+1] += offsets[i]
	}

	neighbors = make([]int32, len(keys))
	labels = make([]uint8, len(keys))

	cursor := make([]int32, numNodes)
	copy(cursor, offsets[:numNodes])

	for i := range keys {
		k := keys[i]
		pos := cursor[k]
		neighbors[pos] = vals[i]
		labels[pos] = labelCodes[i]
		cursor[k]++
	}

	return offsets, neighbors, labels
}
