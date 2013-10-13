package main 

import (
	"fmt"
	"math/rand"
	"time"
	"btree"
)

func doTest(tree *btree.Btree, count int, generate func(int), check func(current, last uint64) bool) bool {

	start := time.Now().UnixNano();	
	for i := 0; i<count; i++ {
		generate(i)
	}
	us := (time.Now().UnixNano() - start) / 1000
	var rate float32 = float32 (((int64(count) * 1000000) / us) / 1000) 
	fmt.Print("\tinsertions took ", us, " us (", rate, " K/sec)\n")

	start = time.Now().UnixNano();	
	ch := tree.Iterate()
	last := <- ch
	for key := range ch {
		if ! check(key,last) {
			fmt.Print("\t[fail]\n")
			return false
		}
		last = key
	}
	us = (time.Now().UnixNano() - start) / 1000
	rate = float32 (((int64(count) * 1000000) / us) / 1000) 
	fmt.Print("\titeration took ", us, " us (", rate, " K/sec)\n")
	fmt.Print("\t[pass]\n")
	return true
}

func main() {

	tree := new (btree.Btree)	
	n := 1000000
	fmt.Print("[test] Linear insertion ", n, " elements\n")
	doTest(tree, n, 
		func(i int) {
			tree.Insert(uint64(i), nil)
		},
		func(current, last uint64) bool {
			return (current == last + 1)
		})

	tree = new (btree.Btree)	
	n = 1000000
	fmt.Print("[test] Random insertion ", n, " elements\n")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	doTest(tree, n, 
		func(i int) {
			tree.Insert(uint64(r.Int63()), nil)
		},
		func(current, last uint64) bool {
			return (current > last)
		})
}

