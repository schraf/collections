package collections

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// csrNeighbor pairs an external neighbor id with its label, for assertions.
type csrNeighbor struct {
	id    int64
	label string
}

// outNeighborsOf returns the external neighbor ids and labels for a node,
// sorted by (id, label) for stable comparison.
func outNeighborsOf(t *testing.T, c *CSR[int64, string], id int64) []csrNeighbor {
	t.Helper()

	dense, ok := c.Dense(id)
	require.True(t, ok, "node %d not found", id)

	var out []csrNeighbor
	nbrs, labels := c.Row(dense)
	for i, nb := range nbrs {
		out = append(out, csrNeighbor{id: c.NodeId(nb), label: c.Label(labels[i])})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].id != out[j].id {
			return out[i].id < out[j].id
		}
		return out[i].label < out[j].label
	})
	return out
}

func TestCSRBuildAndQuery(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "depends_on")
	b.AddEdge(1, 3, "depends_on")
	b.AddEdge(1, 2, "version_of") // parallel edge, different label
	b.AddEdge(2, 3, "built_from")

	c, err := b.Build()
	require.NoError(t, err)

	assert.Equal(t, 3, c.NumNodes())
	assert.Equal(t, 4, c.NumEdges())
	assert.Equal(t, 3, c.NumLabels())

	got := outNeighborsOf(t, c, 1)
	want := []csrNeighbor{
		{2, "depends_on"},
		{2, "version_of"},
		{3, "depends_on"},
	}
	assert.Equal(t, want, got)

	d1, ok := c.Dense(1)
	require.True(t, ok)
	assert.Equal(t, 3, c.Degree(d1))

	d3, ok := c.Dense(3)
	require.True(t, ok)
	assert.Equal(t, 0, c.Degree(d3))
}

func TestCSRRoundTripIdAndLabel(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	b.AddEdge(10, 20, "x")
	c, err := b.Build()
	require.NoError(t, err)

	d, ok := c.Dense(10)
	require.True(t, ok)
	assert.Equal(t, int64(10), c.NodeId(d))

	code, ok := c.LabelCode("x")
	require.True(t, ok)
	assert.Equal(t, "x", c.Label(code))
}

func TestCSRUnknownNodeAndLabel(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "x")
	c, err := b.Build()
	require.NoError(t, err)

	_, ok := c.Dense(99)
	assert.False(t, ok, "node 99 should be absent")

	_, ok = c.LabelCode("nope")
	assert.False(t, ok, "label 'nope' should be absent")

	_, ok = c.LabelCode("x")
	assert.True(t, ok, "label 'x' should be present")
}

func TestCSRAddNodeIsolated(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	b.AddNode(42)
	c, err := b.Build()
	require.NoError(t, err)

	d, ok := c.Dense(42)
	require.True(t, ok, "isolated node 42 missing")
	assert.Equal(t, 0, c.Degree(d))
}

func TestCSRNeighborsIteratorEarlyStop(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "a")
	b.AddEdge(1, 3, "a")
	b.AddEdge(1, 4, "a")
	c, err := b.Build()
	require.NoError(t, err)

	d, ok := c.Dense(1)
	require.True(t, ok)

	count := 0
	for range c.Neighbors(d) {
		count++
		break
	}
	assert.Equal(t, 1, count)
}

func TestCSRNeighborsIteratorFull(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	b.AddEdge(1, 2, "a")
	b.AddEdge(1, 3, "b")
	c, err := b.Build()
	require.NoError(t, err)

	d, _ := c.Dense(1)

	seen := map[int64]string{}
	for nb, label := range c.Neighbors(d) {
		seen[c.NodeId(nb)] = c.Label(label)
	}
	assert.Equal(t, map[int64]string{2: "a", 3: "b"}, seen)
}

func TestCSRTooManyLabels(t *testing.T) {
	b := NewCSRBuilder[int64, int](0, 0)
	for i := 0; i < MaxCSRLabels; i++ {
		b.AddEdge(1, int64(i+2), i)
	}
	// One more distinct label should trip the limit.
	b.AddEdge(1, 99999, MaxCSRLabels)

	_, err := b.Build()
	require.Error(t, err)
}

func TestCSRMaxLabelsExactlyAllowed(t *testing.T) {
	b := NewCSRBuilder[int64, int](0, 0)
	for i := 0; i < MaxCSRLabels; i++ {
		b.AddEdge(1, int64(i+2), i)
	}
	c, err := b.Build()
	require.NoError(t, err)
	assert.Equal(t, MaxCSRLabels, c.NumLabels())
}

func TestCSREmptyBuild(t *testing.T) {
	b := NewCSRBuilder[int64, string](0, 0)
	c, err := b.Build()
	require.NoError(t, err)
	assert.Equal(t, 0, c.NumNodes())
	assert.Equal(t, 0, c.NumEdges())
}
