package graph

import (
	"fmt"

	"github.com/gomodule/redigo/redis"
)

// A policy that takes ownership of the connection.
//
// It uses the connection to load the graph from redis, and on a call to the
// commit function will write back the graph.
//
// if NewOnceOnCommitPolicy returns an error, calling Commit is optional.
// Otherwise, Commit must be called as it will close the connection.
//
// This policy is good for algorithms that would not work on dynamic graphs.
// It is protected with CAS semantics from graph creation to commit. However,
// it can only commit once. It would only make sense to commit once, since it
// only loads the graph once, so it's only guaranteed that the write would even
// be consistent the first time.
//
// This means that it requires exclusive access to the graph while running any
// algorithm if the graph is to be commited.
//
// Redis defines a few protocols that can be used for mutex implementation.
// At the time of writing red-lock is the prefered method, but there is no
// cannonical go implementation that doesn't appear to have issues, so I didn't
// want to add one as a dependency.
//
// In practice, this can be used for algorithms if either: there is a
// guarantee that no one else is using the graph, or the values are not
// writen back to redis.
type onceOnCommitPolicy struct {
	inMemoryPolicy
	graph string
	c     redis.Conn
	dirty map[string]struct{}
}

func NewOnceOnCommitPolicy(graph string, c redis.Conn) (onceOnCommitPolicy, error) {
	odp, err := newOnceOnCommitSubGraph(graph, c, nil)
	if err != nil {
		return odp, err
	}
	err = LoadGraph(odp.c, odp.inMemoryPolicy)
	return odp, err
}

func newOnceOnCommitSubGraph(graph string, c redis.Conn,
	vertices []string) (onceOnCommitPolicy, error) {

	if err := c.Err(); err != nil {
		c.Close()
		return onceOnCommitPolicy{},
			fmt.Errorf("invalid connection (%v)", err)
	}

	odp := onceOnCommitPolicy{
		inMemoryPolicy: NewInMemoryPolicy(graph),
		graph:          graph,
		c:              c,
		dirty:          make(map[string]struct{}),
	}
	err := graphCAS(odp.c, odp.graph)
	if err != nil {
		c.Close()
		return onceOnCommitPolicy{}, err
	}

	if vertices == nil {
		return odp, nil
	}

	err = LoadSubGraph(odp.c, odp.inMemoryPolicy, vertices)
	if err != nil {
		c.Close()
		return odp, err
	}
	return odp, nil
}

func (odp onceOnCommitPolicy) InsertEdges(edges ...InsertEdge) error {
	for _, e := range edges {
		odp.dirty[e.Src] = struct{}{}
		odp.dirty[e.Dst] = struct{}{}
	}
	return odp.inMemoryPolicy.InsertEdges(edges...)
}

func (odp onceOnCommitPolicy) RemoveEdges(edges ...RemoveEdge) error {
	for _, e := range edges {
		odp.dirty[e.Src] = struct{}{}
		odp.dirty[e.Dst] = struct{}{}
	}
	return odp.inMemoryPolicy.RemoveEdges(edges...)
}

func (odp onceOnCommitPolicy) InsertVertices(vertices ...string) error {
	for _, v := range vertices {
		odp.dirty[v] = struct{}{}
	}
	return odp.inMemoryPolicy.InsertVertices(vertices...)
}

func (odp onceOnCommitPolicy) RemoveVertices(vertices ...string) error {
	for _, v := range vertices {
		odp.dirty[v] = struct{}{}
	}
	return odp.inMemoryPolicy.RemoveVertices(vertices...)
}

// Commit commits changes if possible. Commiting is always correct, though it
// may return an error if commiting was not possible.
func (odp onceOnCommitPolicy) Commit() error {
	defer odp.c.Close()
	err := checkCAS(odp.c)
	if err != nil {
		return err
	}

	// Only need to write modified parts of graph.
	temp := NewInMemoryPolicy(odp.graph)
	for v := range odp.dirty {
		temp.m[v] = odp.inMemoryPolicy.m[v].Copy()
	}

	err = StoreGraph(odp.c, temp)
	if err != nil {
		fmt.Errorf("graph %s: %v", odp.graph, err)
	}
	v, err := setCAS(odp.c)
	if err != nil {
		return fmt.Errorf("graph %s: %v: values: %v", odp.graph, err, v)
	}
	return nil
}
