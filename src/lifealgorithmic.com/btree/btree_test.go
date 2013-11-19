package btree

import (
 	"testing"
	"math/rand"
	"time"
	"runtime"
//	"fmt"
)

type Treelike interface {
	Insert(uint64, interface{})
	Fetch(uint64) interface{}
	Delete(uint64)
	Iterate() chan uint64
}

type BtreeTest struct {
	test *testing.T
	tree Treelike 
	reference map[uint64] interface{}	
}

func (self *BtreeTest) Insert(key uint64, value interface{}) {
	self.reference[key] = value
	self.tree.Insert(key, value)
}

func (self *BtreeTest) Fetch(key uint64) interface{} {
	value := self.tree.Fetch(key)
	if (value != self.reference[key]) {
		self.test.Fail()
	}
	return value
}

func (self *BtreeTest) Delete(key uint64) {
	delete(self.reference, key)
	self.tree.Delete(key)
	
	verify := self.tree.Fetch(key) 
	if (verify != nil) {
		self.test.Fail()
	}
}

func (self *BtreeTest) Iterate() chan uint64 {
	rval := make (chan uint64) 
	treechan := self.tree.Iterate()
	checker := func() {
		var lastkey uint64
		var checklast bool = false
		for key := range treechan {
			if (checklast) {
				if key < lastkey {
					self.test.Fail()
				}
			}
			checklast = true
			lastkey = key
			
			_, ok := self.reference[key]
			if (!ok) {
				self.test.Fail()
			}
			
			rval <- key
		}
		close(rval)
	}
	go checker()
	return rval
}

func RandomTest(t *testing.T, tree Treelike, seed int64, iterations int, insertions int) {
	deletions := 5 * insertions
	t.Log("Random seed:", seed)

	src := rand.NewSource(seed)
	test := new (BtreeTest)
	test.test = t
	test.reference = make (map[uint64] interface{})
	test.tree = tree

	keys := make([] uint64, insertions*iterations, insertions*iterations);

	for i:=0; i<iterations; i++ {

		for j:=0; j<insertions; j++ {
			key := uint64(src.Int63())
			test.Insert(key, j)
			keys[(insertions*i)+j] = key
		}
	
		for j:=0; j<deletions; j++ {
			listindex := uint(src.Int63()) % uint(insertions*(i+1))
			test.Delete(keys[listindex])
		}
	}
	
	ch := test.Iterate()
	for key := range ch {
		key = key + 1
	}
	
	t.Log("stats size:", tree.(*Btree).Stats.Size)
	t.Log("stats depth:", tree.(*Btree).Stats.Depth)
	t.Log("stats nodes:", tree.(*Btree).Stats.Nodes)
	t.Log("stats leaves:", tree.(*Btree).Stats.Leaves)
}

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
	
	for order := range orders {
		tree := Create(order)
		seed := time.Now().UnixNano()
		RandomTest(t, tree, seed, iterations, insertions)
		runtime.GC()
	}
}

