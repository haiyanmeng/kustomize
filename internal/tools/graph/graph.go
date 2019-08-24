// This package implements a weighted directed graph that is persistent through
// the use of Redis. The implementation expects a reasonably sparse graph and
// provides an interface that allows for the redis communication/persistence
// preferences to be configurable for eventual performance considerations.
package graph

import (
	"log"
	"os"
)

// Check that interfaces are satisfied.
var (
	_ = NewGraph(inMemoryPolicy{})
	_ = NewGraph(onceOnCommitPolicy{})
)

// The point of this interface is to facilitate the testing of graph algorithms
// without needing to run redis.
//
// It can also serve to be used for different persistence policies. For
// instance, one instance could lazy load the graph data, while another can
// completely read the graph into memory.
//
// Another factor to consider, is failure and failure recovery. One policy could
// write each edge to a list of edges, and make edits to the vertices edges
// consistent on Commit(). Since this data would be stored in redis, if a
// computation goes wrong, the state can be recovered and commited on the next
// computation. However, this does not always make sense if partial computations
// would break semmantics of the data, in which case an implementation that only
// writes on Commit(s) would make sense.
//
// The interface is meant to be used in the following order:
//
//     // Policy decides to load the graph as it wishes.
//     policy, err := graph.NewPolicy(name, redisConn)
//     if err != nil {
//		// handle.
//     }
//     graph := graph.NewGraph(policy)
//
//     // Decide whether or not to enforce commits on failure.
//     defer graph.Commit()
//
//     // Use the graph for a purpose.
//
// The inMemoryPolicy is meant to be used as the underlying backbone of each
// implementation.
type StoragePolicy interface {
	// Immutable values.
	Name() string
	Vertices() ([]string, error)
	Edges(string) (Edges, bool, error)

	// Mutator methods.
	InsertEdges(...InsertEdge) error
	RemoveEdges(...RemoveEdge) error
	InsertVertices(...string) error
	RemoveVertices(...string) error

	// Part of the persistence API.
	Commit() error
}

type edgeType int

const (
	base edgeType = iota
	resource
	patch
)

var (
	logger = log.New(os.Stdout, "redis graph: ",
		log.LstdFlags|log.Llongfile|log.LUTC)
)

type EdgeValue struct {
	W float64  `json:"weight"`
	T edgeType `json:"edgeType"`
}

type InsertEdge struct {
	Src string    `json:"src"`
	Dst string    `json:"dst"`
	Val EdgeValue `json:"edgeValue"`
}
type RemoveEdge struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

type Edges struct {
	InEdges  map[string]EdgeValue `json:"inEdges,omitempty"`
	OutEdges map[string]EdgeValue `json:"outEdges,omitempty"`
}

func (es Edges) InitIfEmpty() {
	if es.InEdges == nil {
		es.InEdges = make(map[string]EdgeValue)
	}
	if es.OutEdges == nil {
		es.OutEdges = make(map[string]EdgeValue)
	}
}

func (es Edges) Copy() Edges {
	cpy := Edges{}
	cpy.InitIfEmpty()

	if es.InEdges != nil {
		for k, v := range es.InEdges {
			cpy.InEdges[k] = v
		}
	}

	if es.OutEdges != nil {
		for k, v := range es.OutEdges {
			cpy.OutEdges[k] = v
		}
	}
	return cpy
}

type Graph struct {
	StoragePolicy
}

func NewGraph(sp StoragePolicy) Graph {
	return Graph{sp}
}
