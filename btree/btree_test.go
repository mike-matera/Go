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
	reference map[uint64] int
}

func (self *BtreeTest) Put(key interface{}, value interface{}) {
	self.reference[key.(uint64)] = value.(int)
	self.tree.Put(key, value)
	self.tree.Check(self.test)
}

func (self *BtreeTest) Get(key interface{}) interface{} {
	value := self.tree.Get(key)
	if (value != self.reference[key.(uint64)]) {
		self.test.Error("Fetch(): Mismatch:", value, "!=", self.reference[key.(uint64)])
	}
	return value
}

func (self *BtreeTest) Delete(key interface{}) {
	delete(self.reference, key.(uint64))
	self.tree.Delete(key)
	
	verify := self.tree.Get(key) 
	if (verify != nil) {
		self.test.Error("Delete(): Value was not deleted:", key)
		self.test.FailNow()
	}
	self.tree.Check(self.test)
}

func (self *BtreeTest) Iterate() chan Entry {
	rval := make (chan Entry) 
	treechan := self.tree.Iterate()
	checker := func() {
		var lastkey uint64
		var checklast bool = false
		for entry := range treechan {
			if (checklast) {
				if entry.Key.(uint64) < lastkey {
					self.test.Error("Iterate(): Values are not increasing:", entry.Key, ">=", lastkey)
				}
			}
			checklast = true
			lastkey = entry.Key.(uint64)
			
			_, ok := self.reference[entry.Key.(uint64)]
			if (!ok) {
				self.test.Error("Iterate(): Iteration produced a false key:", entry.Key)
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
	test.reference = make (map[uint64] int)
	test.tree = tree

	keys := make([] uint64, insertions*iterations, insertions*iterations);

	for i:=0; i<iterations; i++ {

		for j:=0; j<insertions; j++ {
			key := uint64(src.Int63())
			test.Put(key, j)
			keys[(insertions*i)+j] = key
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
		dummy += key.Key.(uint64)
		count++
	}
	if count == 0 {
		test.test.Error("Iteration produced no items!")
	}
}

func TestAutoRandomBTree(t *testing.T) {
	order := 4
	iterations := 2
	insertions := 1000
	tree := NewBTree(order)
	seed := time.Now().UnixNano()
	RandomTest(t, tree, seed, iterations, insertions)
}

func TestRandom(t *testing.T) {
	orders := [] int {4, 8, 16, 32, 64, 128}
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
