package graph

import (
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

const (
	graphsContents = "graphs:contents:"
	// eventually add graphsMutex = "graph:mutex:"
)

func UnmarshalEdges(edgeData string) (Edges, error) {
	var es Edges
	es.InitIfEmpty()
	err := json.Unmarshal([]byte(edgeData), &es)
	return es, err
}

func MarshalEdges(edges Edges) (string, error) {
	data, err := json.Marshal(edges)
	return string(data), err
}

func contents(name string) string {
	return graphsContents + name
}

func redisStringInput(graph string, keys []string) []interface{} {
	params := make([]interface{}, len(keys)+1)
	params[0] = contents(graph)
	for i, v := range keys {
		params[i+1] = v
	}
	return params
}

func LoadSubGraph(c redis.Conn, mem inMemoryPolicy, keys []string) error {
	params := redisStringInput(mem.Name(), keys)
	es, err := redis.Values(c.Do("HMGET", params...))
	if err != nil {
		return err
	}

	for i, e := range es {
		if e == nil {
			continue
		}
		str, ok := e.(string)
		if !ok {
			return fmt.Errorf("unexpected value in graph %#v", e)
		}

		edges, err := UnmarshalEdges(str)
		if err != nil {
			return fmt.Errorf(
				"error could not parse edges %s in vertex %s from graph %s: %v\n",
				e, keys[i], mem.Name(), err)
		}
		mem.m[keys[i]] = edges
	}

	return nil
}

func ReadVertices(c redis.Conn, graph string) ([]string, error) {
	return redis.Strings(c.Do("HKEYS", contents(graph)))
}

func LoadGraph(c redis.Conn, mem inMemoryPolicy) error {
	data, err := redis.Strings(c.Do("HGETALL", contents(mem.Name())))
	if err != nil {
		return fmt.Errorf("Could not read response: %v", err)
	}
	if len(data)%2 != 0 {
		return fmt.Errorf("should have a key for every value")
	}

	for i := 0; i+1 < len(data); i += 2 {
		v := data[i]
		es := data[i+1]
		edges, err := UnmarshalEdges(es)
		if err != nil {
			// To recover from this case, it is definitely possible to
			// regenerate the graph from the elasticsearch index.
			//
			// ... It may also be possible to regenerate this by looking at
			// all other edges in the case where the unparseable edges are
			// in disjoint sets... But its difficult to know this without
			// dynamically storing a union-find structure in redis. I also don't
			// think rebuilding a graph like this would take too too long, so
			// I think rebuilding is the best option.
			//
			// This is a pretty weird failure case. A rollback may also be a
			// good idea.
			return fmt.Errorf(
				"error could not parse edges %s in vertex %s from graph %s: %v\n",
				es, v, mem.Name(), err)
		}
		mem.m[v] = edges
	}

	return nil
}

func StoreGraph(c redis.Conn, mem inMemoryPolicy) error {
	if len(mem.m) == 0 {
		return nil
	}

	pairs := make([]string, 2*len(mem.m))
	i := 0
	for v, es := range mem.m {
		pairs[i] = v
		data, err := MarshalEdges(es)
		if err != nil {
			return fmt.Errorf("Could not marshal values commit aborted: %v", err)
		}
		pairs[i+1] = data
		i += 2
	}

	graph := mem.Name()
	_, err := c.Do("HMSET", redisStringInput(mem.Name(), pairs)...)
	if err != nil {
		return fmt.Errorf("could not write to graph %s: %v", graph, err)
	}
	return nil
}

func removeVertices(c redis.Conn, graph string, toDelete []string) (int, error) {
	params := redisStringInput(contents(graph), toDelete)
	return redis.Int(c.Do("HDEL", params))
}

// The following methods are part of the redis idiom for doing check-and-set
// transactions that guarantee consistency between read-and-write operations
// They don't guarantee that the operation executes, so you may need to retry
// the operation util there is no competition for the write
func graphCAS(c redis.Conn, graph string) error {
	return startCAS(c, contents(graph))
}

func startCAS(c redis.Conn, watchKeys ...string) error {
	_, err := c.Do("WATCH", watchKeys)
	if err != nil {
		return fmt.Errorf("could not start check-and-set: %v", err)
	}
	return nil
}

func checkCAS(c redis.Conn) error {
	_, err := c.Do("MULTI")
	if err != nil {
		return fmt.Errorf("could not start write operations: %v", err)
	}
	return nil
}

func setCAS(c redis.Conn) (interface{}, error) {
	ifc, err := c.Do("EXEC")
	if err != nil {
		return ifc, fmt.Errorf("could not commit cas operations: %v", err)
	}
	return ifc, nil
}
