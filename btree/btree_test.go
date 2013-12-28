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
	if self.reference[key.(uint64)] != self.tree.Get(key) {
		self.test.Error("Put(): Mismatch:", self.tree.Get(key), "!=", self.reference[key.(uint64)])
		self.test.FailNow()
	} 
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
			
			refval, ok := self.reference[entry.Key.(uint64)]
			if (!ok) {
				self.test.Error("Iterate(): Iteration produced a false key:", entry.Key)
			}
			
			if refval != entry.Value.(int) {
				self.test.Error("Iterate(): Iteration discovered a false value:", entry.Value)
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
			keys[(insertions*i)+j] = key			
			test.Put(key, j)
			// Double insert every so often...
			if (j % 100) == 10 {
				test.Put(key, j+1)
			}
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

func xTestAutoRandomBTree(t *testing.T) {
	order := 4
	iterations := 2
	insertions := 1000
	tree := NewBTree(order)
	seed := time.Now().UnixNano()
	RandomTest(t, tree, seed, iterations, insertions)
}

func xTestRandom(t *testing.T) {
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

func BenchmarkRandomInsertions(b *testing.B) {
	order := 16
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	tree := NewBPlusTree(order)

    b.ResetTimer()
	for j:=0; j<b.N; j++ {
		tree.Put(uint64(src.Int63()), j)
	}	
}

func BenchmarkRandomGet(b *testing.B) {
	order := 16
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)
	tree := NewBPlusTree(order)

	for j:=0; j<b.N; j++ {
		tree.Put(uint64(src.Int63()), j)
	}	
	
    b.ResetTimer()
	src = rand.NewSource(seed)
	for j:=0; j<b.N; j++ {
		key := uint64(src.Int63())
		k := tree.Get(key)
		if k != j {
			b.Error("Mismatched value:", k)
		}
	}	
	
}
