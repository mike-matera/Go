package btree

import (
    "testing"
	"runtime"
	"time"
	"math/rand"
	"github.com/cznic/b"
)

type CznicAdapter struct {
	tree *b.Tree	
}

func cmp(a, b uint64) int {
	if a < b {
		return -1
	}else if (a > b) {
		return 1
	}else{
		return 0
	}
}

func CznicTree() *CznicAdapter {
	rval := new (CznicAdapter)
	rval.tree = b.TreeNew(cmp)
	return rval
}
	
func (self *CznicAdapter) Put(key interface{}, value interface{}) {
	self.tree.Set(key.(uint64), value.(int))
}

func (self *CznicAdapter) Get(key interface{}) interface{} {
	rval,okay := self.tree.Get(key.(uint64)) 
	if okay {
		return rval
	}else{
		return nil
	}
}

func (self *CznicAdapter) Delete(key interface{}) {
	self.tree.Delete(key.(uint64))
}

func (self *CznicAdapter) Iterate() chan Entry {
	return nil
}

func (self *CznicAdapter) Check(t *testing.T) {
}

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
	if treechan == nil {
		return nil
	}
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
	if ch != nil {
		count := 0
		for entry := range ch {
			value := test.reference[entry.Key.(uint64)]
			if (value != entry.Value.(int)) {
				t.Error("Iterate(): Iteration discovered a false value:", entry.Value)
				t.FailNow()
			}
			count += 1
		}
		if count == 0 {
			test.test.Error("Iteration produced no items!")
		}
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

func TestRandomCznic(t *testing.T) {
	iterations := 10
	insertions := 1000
	
	tree := CznicTree()
	seed := time.Now().UnixNano()
	RandomTest(t, tree, seed, iterations, insertions)
}

func SetupBenchmark(b *testing.B, tree Treelike) (int64, rand.Source) {
	prefill := 3000000
	seed := time.Now().UnixNano()
	src := rand.NewSource(seed)

	/*
	if prefill < b.N {
		prefill = b.N
	}
	*/
	
	for j:=0; j<prefill; j++ {
		tree.Put(uint64(src.Int63()), j)
	}	

	return seed, src
}

func BenchmarkRandomPut(b *testing.B) {
	order := 128
	tree := NewBPlusTree(order)
	_, src := SetupBenchmark(b, tree)

    b.ResetTimer()
	for j:=0; j<b.N; j++ {
		tree.Put(uint64(src.Int63()), j)
	}	
}

func BenchmarkRandomGet(b *testing.B) {
	order := 128
	tree := NewBPlusTree(order)
	seed, src := SetupBenchmark(b, tree)

    b.ResetTimer()
	src = rand.NewSource(seed)
	for j:=0; j<b.N; j++ {
		tree.Get(uint64(src.Int63()))
	}
}

func BenchmarkRandomDelete(b *testing.B) {
	order := 128
	tree := NewBPlusTree(order)
	seed, src := SetupBenchmark(b, tree)

    b.ResetTimer()
	src = rand.NewSource(seed)
	for j:=0; j<b.N; j++ {
		tree.Delete(uint64(src.Int63()))
	}
}

/*
func BenchmarkIteration(b *testing.B) {
	order := 128
	tree := NewBPlusTree(order)
	SetupBenchmark(b, tree)

    b.ResetTimer()
	ch := tree.Iterate()
	for _ = range (ch) {
	}
}
*/

func BenchmarkCznicRandomPut(b *testing.B) {
	tree := CznicTree()
	_, src := SetupBenchmark(b, tree)

    b.ResetTimer()
	for j:=0; j<b.N; j++ {
		tree.Put(uint64(src.Int63()), j)
	}	
}

func BenchmarkCznicRandomGet(b *testing.B) {
	tree := CznicTree()
	seed, src := SetupBenchmark(b, tree)

    b.ResetTimer()
	src = rand.NewSource(seed)
	for j:=0; j<b.N; j++ {
		tree.Get(uint64(src.Int63()))
	}
}

func BenchmarkCznicRandomDelete(b *testing.B) {
	tree := CznicTree()
	seed, src := SetupBenchmark(b, tree)

    b.ResetTimer()
	src = rand.NewSource(seed)
	for j:=0; j<b.N; j++ {
		tree.Delete(uint64(src.Int63()))
	}
}

/*
func BenchmarkCznicIteration(b *testing.B) {
	tree := CznicTree()
	_, src := SetupBenchmark(b, tree)

    b.ResetTimer()
	ch := tree.Iterate()
	for _ = range (ch) {
	}
}
*/