package main 

import (
	"fmt"
	"math/rand"
	"time"
	"btree"
)

func doBasicTest(count int, 
				insert func (int),
				channel func () (chan uint64),
				check func(uint64, uint64) (bool),
				fetch func(int) bool ) (bool, int32, int32, int32) {

	start := time.Now().UnixNano();	
	for i := 0; i<count; i++ {
		insert(i)
	}
	us := (time.Now().UnixNano() - start) / 1000
	insrate := int32(((int64(count) * 1000000) / us) / 1000) 
	fmt.Print("\tinsertions took ", us, " us (", insrate, " K/sec)\n")

	start = time.Now().UnixNano()
	ch := channel()
	last := <- ch
	for key := range ch {
		if ! check(key,last) {
			fmt.Print("\t[fail]\n")
			return false,0,0,0
		}
		last = key
	}
	us = (time.Now().UnixNano() - start) / 1000
	iterrate := int32 (((int64(count) * 1000000) / us) / 1000) 
	fmt.Print("\titeration took ", us, " us (", iterrate, " K/sec)\n")

	start = time.Now().UnixNano()
	for i := 0; i<count; i++ {
		if ! fetch(i) {
			fmt.Print("\t[fail]\n")
			return false,0,0,0
		}
	}
	us = (time.Now().UnixNano() - start) / 1000
	fetchrate := int32 (((int64(count) * 1000000) / us) / 1000) 
	fmt.Print("\tfetch took ", us, " us (", fetchrate, " K/sec)\n")
	return true,insrate,iterrate,fetchrate
}

func lineartest (count, order int) (int32, int32, int32) {
	tree := btree.Create(order)
	fmt.Print("[test] Linear insertion ", count, " elements (order ", order, ")\n")
	pass,ir,itr,fr := doBasicTest(count, 
		func (i int) {
			tree.Insert(uint64(i), i)
		},
		func () (ch chan uint64) {
			return tree.Iterate()
		},
		func (current uint64, last uint64) (bool) {
			return (current == last + 1)
		},
		func (i int) bool {
			got := tree.Fetch(uint64(i))
			if (got == nil || got.(int) != i) {
				return false
			}
			return true
		})
	if pass {
		fmt.Print("\t[pass]: depth: ", tree.Stats.Depth, " nodes: ", tree.Stats.Nodes, " leaves: ", tree.Stats.Leaves, "\n\n") 
	}else{
		fmt.Print("\t[fail]\n\n")
	}
	return ir,itr,fr
}

func perftest (count, order int) (int32, int32, int32) {
	tree := btree.Create(order)
	fmt.Print("[test] Random insertion ", count, " elements (order ", order, ")\n")
	seed := time.Now().UnixNano()
	r := rand.NewSource(seed)
	pass,ir,itr,fr := doBasicTest(count,
		func(i int) {
			number := uint64(r.Int63())
			tree.Insert(number, int(number))
		},
		func() (ch chan uint64) {
			r.Seed(seed)
			return tree.Iterate()
		},
		func (current uint64, last uint64) (bool) {
			return (current >= last)
		},
		func (i int) bool {
			number := uint64(r.Int63())
			got := tree.Fetch(number)
			if (got == nil || got.(int) != int(number)) {
				return false
			}
			return true
		})
	if pass {
		fmt.Print("\t[pass]: depth: ", tree.Stats.Depth, " nodes: ", tree.Stats.Nodes, " leaves: ", tree.Stats.Leaves, "\n\n") 
	}else{
		fmt.Print("\t[fail]\n\n")
	}
	return ir,itr,fr
}

type Result struct {
	InsertionRate int32
	IterationRate int32
	FetchRate int32
}

func main() {
	results := make (map[int] map[int] Result)

	for _,order := range([] int{4, 8, 16, 32, 64}) {
		results[order] = make (map[int] Result)
		for count := 500000; count <= 3000000; count += 500000 {
			var r Result
			r.InsertionRate, r.IterationRate, r.FetchRate = perftest(count,order)
			results[order][count] = r
		}
	}

	for order,m := range( results ) {
		for count, result := range ( m ) {
			fmt.Print(order, ",", count, ",", result.InsertionRate, ",", result.FetchRate, ",", result.IterationRate, "\n")			
		}
	}
}
