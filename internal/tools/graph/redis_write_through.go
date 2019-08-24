package graph

import (
	"github.com/gomodule/redigo/redis"
)

type writeThroughPolicy struct {
	graph string
	pool  *redis.Pool
}

// This policy is good for insertions to the graph. The execution within each
// function will be consistent and protected with CAS semantics, so if it
// fails you should be able to restart the operation (as long as redis is still
// connected/live).
//
// It would also work fine as a read-only dyncamic graph.
// It is also implemented to work concurrently correctly.
func NewWriteThroughPolicy(graph string,
	pool *redis.Pool) writeThroughPolicy {

	return writeThroughPolicy{
		graph: graph,
		pool:  pool,
	}
}

func (wtp writeThroughPolicy) Name() string {
	return wtp.graph
}

func (wtp writeThroughPolicy) Vertices() ([]string, error) {
	c := wtp.pool.Get()
	defer c.Close()
	return ReadVertices(c, wtp.graph)
}

func (wtp writeThroughPolicy) Edges(vertex string) (Edges, bool, error) {
	g, err := wtp.newSubGraph([]string{vertex})
	if err != nil {
		return Edges{}, false, err
	}
	defer g.Commit()
	return g.Edges(vertex)
}

func (wtp writeThroughPolicy) newSubGraph(
	vertices []string) (onceOnCommitPolicy, error) {

	c := wtp.pool.Get()
	return newOnceOnCommitSubGraph(wtp.graph, c, vertices)
}

func (wtp writeThroughPolicy) InsertEdges(edges ...InsertEdge) error {
	valSet := make(map[string]struct{})
	for _, edge := range edges {
		valSet[edge.Src] = struct{}{}
		valSet[edge.Dst] = struct{}{}
	}

	vs := make([]string, len(valSet))
	i := 0
	for k := range valSet {
		vs[i] = k
		i++
	}
	g, err := wtp.newSubGraph(vs)
	if err != nil {
		return err
	}
	g.InsertEdges(edges...)
	return g.Commit()
}

func (wtp writeThroughPolicy) RemoveEdges(edges ...RemoveEdge) error {
	valSet := make(map[string]struct{})
	for _, edge := range edges {
		valSet[edge.Src] = struct{}{}
		valSet[edge.Dst] = struct{}{}
	}

	vs := make([]string, len(valSet))
	i := 0
	for k := range valSet {
		vs[i] = k
		i++
	}
	g, err := wtp.newSubGraph(vs)
	if err != nil {
		return err
	}
	g.RemoveEdges(edges...)
	return g.Commit()
}

func (wtp writeThroughPolicy) RemoveVertices(toRemove ...string) error {
	c := wtp.pool.Get()
	defer c.Close()

	cnt, err := removeVertices(c, wtp.graph, toRemove)
	if err != nil {
		return err
	}
	unique := make(map[string]struct{})
	for _, v := range toRemove {
		unique[v] = struct{}{}
	}
	if len(unique) != cnt {
		logger.Printf(
			"removing %#v (of size %d) but deleted %d instead\n",
			toRemove, len(unique), cnt)
	}
	return nil
}

func (wtp writeThroughPolicy) InsertVertices(toInsert ...string) error {
	g, err := wtp.newSubGraph([]string(toInsert))
	if err != nil {
		return nil
	}
	g.InsertVertices(toInsert...)
	return g.Commit()
}

func (wtp writeThroughPolicy) Commit() error {
	return wtp.pool.Close()
}
