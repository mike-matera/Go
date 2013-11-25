package btree

import (
 	"testing"
	"runtime"
	"time"
//	"fmt"
)

func TestAutoRandom(t *testing.T) {
	order := 4
	iterations := 2
	insertions := 1000
	tree := Create(order)
	seed := time.Now().UnixNano()
	RandomTest(t, tree, seed, iterations, insertions)
}

func TestRandom(t *testing.T) {
	orders := [] int {4, 8, 16, 32}
	iterations := 10
	insertions := 1000
	
	var tree * Btree;
	for _,order := range orders {
		tree = Create(order)
		runtime.GC()
		seed := time.Now().UnixNano()
		RandomTest(t, tree, seed, iterations, insertions)
	}
	
	/*
	t.Log("stats size:", tree.Stats.Size)
	t.Log("stats depth:", tree.Stats.Depth)
	t.Log("stats nodes:", tree.Stats.Nodes)
	t.Log("stats leaves:", tree.Stats.Leaves)	
	*/
}

