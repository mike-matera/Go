package btree

import (
    "testing"
	"runtime"
	"time"
	"math/rand"
)

type BtreeTest struct {
	test *testing.T
	tree Treelike 
	reference map[uint64] Pair
}

func (self *BtreeTest) Put(item interface{}) {
	self.reference[item.(Pair).Key] = item.(Pair)
	self.tree.Put(item)
	self.tree.Check(self.test)
}

func (self *BtreeTest) Get(item interface{}) interface{} {
	value := self.tree.Get(item.(Pair))
	if (value != self.reference[item.(Pair).Key]) {
		self.test.Error("Fetch(): Mismatch:", value, "!=", self.reference[item.(Pair).Key])
	}
	return value
}

func (self *BtreeTest) Delete(key interface{}) {
	delete(self.reference, key.(Pair).Key)
	self.tree.Delete(key)
	
	verify := self.tree.Get(key) 
	if (verify != nil) {
		self.test.Error("Delete(): Value was not deleted:", key)
		self.test.FailNow()
	}
	self.tree.Check(self.test)
}

func (self *BtreeTest) Iterate() chan interface{} {
	rval := make (chan interface{}) 
	treechan := self.tree.Iterate()
	checker := func() {
		var lastkey uint64
		var checklast bool = false
		for entry := range treechan {
			if (checklast) {
				if entry.(Pair).Key < lastkey {
					self.test.Error("Iterate(): Values are not increasing:", entry.(Pair).Key, ">=", lastkey)
				}
			}
			checklast = true
			lastkey = entry.(Pair).Key
			
			_, ok := self.reference[entry.(Pair).Key]
			if (!ok) {
				self.test.Error("Iterate(): Iteration produced a false key:", entry.(Pair).Key)
			}
			
			rval <- entry
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
	test.reference = make (map[uint64] Pair)
	test.tree = tree

	keys := make([] Pair, insertions*iterations, insertions*iterations);

	for i:=0; i<iterations; i++ {

		for j:=0; j<insertions; j++ {
			item := Pair{uint64(src.Int63()), j}
			test.Put(item)
			keys[(insertions*i)+j] = item
		}
	
		for j:=0; j<deletions; j++ {
			listindex := uint(src.Int63()) % uint(insertions*(i+1))
			test.Delete(keys[listindex])
		}
	}
	
	ch := test.Iterate()
	count := 0
	dummy := uint64(0)
	for key := range ch {
		dummy += key.(Pair).Key
		count++
	}
	if count == 0 {
		test.test.Error("Iteration produced no items!")
	}
}

/*
func TestAutoRandomBTree(t *testing.T) {
	order := 4
	iterations := 2
	insertions := 1000
	tree := NewBTree(order)
	seed := time.Now().UnixNano()
	RandomTest(t, tree, seed, iterations, insertions)
}

func TestRandom(t *testing.T) {
	orders := [] int {4, 8, 16, 32}
	iterations := 10
	insertions := 1000
	
	var tree * BTree;
	for _,order := range orders {
		tree = NewBTree(order)
		runtime.GC()
		seed := time.Now().UnixNano()
		RandomTest(t, tree, seed, iterations, insertions)
	}
}
*/
func TestAutoRandomBplus(t *testing.T) {
	order := 4
	iterations := 2
	insertions := 100
	tree := NewBPlusTree(order)
	seed := time.Now().UnixNano()
	RandomTest(t, tree, seed, iterations, insertions)
}

func TestRandomBplus(t *testing.T) {
	orders := [] int {4, 8, 16, 32, 64, 128}
	iterations := 10
	insertions := 1000
	
	var tree * BPlusTree;
	for _,order := range orders {
		tree = NewBPlusTree(order)
		runtime.GC()
		seed := time.Now().UnixNano()
		RandomTest(t, tree, seed, iterations, insertions)
	}
}
