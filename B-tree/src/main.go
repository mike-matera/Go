package main 

import (
	"fmt"
	"math/rand"
	"time"
	"btree"
)

func iteratorCheck(tree *btree.Btree, check func(current, last uint64) bool) (bool, int64) {
	start := time.Now().UnixNano();	
	ch := tree.Iterate()
	last := <- ch
	for key := range ch {
		if ! check(key,last) {
			return false, (time.Now().UnixNano() - start)/1000
		}
		last = key
	}
	return true, (time.Now().UnixNano() - start)/1000
}

func doTest(tree *btree.Btree, count int, generate func(int, *btree.Btree), check func(current, last uint64) bool) {

	start := time.Now().UnixNano();	
	for i := 0; i<count; i++ {
		generate(i, tree)
	}
	end := time.Now().UnixNano()
	us := (end - start) / 1000
	var rate float32 = float32 (((int64(count) * 1000000) / us) / 1000) 
	fmt.Print("\tinsertions took ", us, " us (", rate, " K/sec)\n")
	pass, us := iteratorCheck(tree,check)
	rate = float32 (((int64(count) * 1000000) / us) / 1000) 
	if pass {
		fmt.Print("\titeration took ", us, " us (", rate, " K/sec)\n")
		fmt.Print("\t[pass]\n")
	}else{
		fmt.Print("\t[fail]\n")
	}	
}

func main() {
	tree := new (btree.Btree)
	
	n := 500000
	fmt.Print("[test] Linear insertion ", n, " elements\n")
	doTest(tree, n, 
		func(i int, t *btree.Btree) {
			t.Insert(uint64(i), nil)
		},
		func(current, last uint64) bool {
			return (current == last + 1)
		})

	fmt.Print("[test] Random insertion ", n, " elements\n")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	doTest(tree, n, 
		func(i int, t *btree.Btree) {
			t.Insert(uint64(r.Int63()), nil)
		},
		func(current, last uint64) bool {
			return (current > last)
		})
}

