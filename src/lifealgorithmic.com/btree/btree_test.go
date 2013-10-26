package btree

import (
    "testing"
	"math/rand"
	"time"
	"fmt"
)

type TreeTest interface {
	Insert (int)
	Delete (int)
	Channel () (chan uint64)
	Check (uint64, uint64) (bool)
	Fetch (int) bool
}

type RandomTest struct {
	seed int64
	src rand.Source
	tree *Btree
	list [] uint64
	kill map[uint64] int
	killlist [] uint64
}

func Generate ( order, insertions, deletions int, seed int64 )  * RandomTest {
	test := new (RandomTest)
	test.list = make ([] uint64, insertions, insertions)
	test.kill = make (map[uint64] int)
	test.killlist = make ([] uint64, deletions, deletions)
	test.seed = seed
	test.tree = Create(order)
	test.src = rand.NewSource(test.seed)
	
	for i:=0; i<insertions; i++ {
		test.list[i] = uint64(test.src.Int63())
	}
	
	for i:=0; i<deletions; i++ {
		listindex := uint(test.src.Int63()) % uint(insertions)
		test.kill[test.list[listindex]] = 1 
		test.killlist[i] = test.list[listindex] 
	}
	
	return test	
}

func (self *RandomTest) Insert (i int) {
	self.tree.Insert(self.list[i], i)
}

func (self *RandomTest) Delete (i int) {
	self.tree.Delete(self.killlist[i])
}

func (self *RandomTest) Channel() (ch chan uint64) {
	return self.tree.Iterate()
}

func (self *RandomTest) Check(current uint64, last uint64) (bool) {
	return (current >= last)
}

func (self *RandomTest) Fetch (i int) bool {
	got := self.tree.Fetch(self.list[i])
	if (got == nil) {
		_, ok := self.kill[self.list[i]]
		return ok
	}
	return (got.(int) == int(i))
}

type LinearTest struct {
	tree *Btree
}

func (self *LinearTest) Insert (i int) {
	self.tree.Insert(uint64(i), i)
}

func (self *LinearTest) Delete (i int) {
	self.tree.Delete(uint64(i))
}

func (self *LinearTest) Channel() (ch chan uint64) {
	return self.tree.Iterate()
}

func (self *LinearTest) Check(current uint64, last uint64) (bool) {
	return (current > last)
}

func (self *LinearTest) Fetch (i int) bool {
	got := self.tree.Fetch(uint64(i))
	if (got == nil || got.(int) == i) {
		return true
	}
	return false
}

func doTest(insertions int, deletions int, test TreeTest) (bool,int32,int32,int32) {

	start := time.Now().UnixNano();	
	for i := 0; i<insertions; i++ {
		test.Insert(i)
	}
	us := (time.Now().UnixNano() - start) / 1000
	insrate := int32(((int64(insertions) * 1000000) / us) / 1000) 

//	start := time.Now().UnixNano();	
	for i := 0; i<deletions; i++ {
		test.Delete(i)
	}
//	us := (time.Now().UnixNano() - start) / 1000
//	delrate := int32(((int64(deletions) * 1000000) / us) / 1000) 

	start = time.Now().UnixNano()
	ch := test.Channel()
	last := <- ch
	for key := range ch {
		if ! test.Check(key,last) {
			fmt.Println("Failed in Check(", key, last, ")")
			return false,0,0,0
		}
		last = key
	}
	us = (time.Now().UnixNano() - start) / 1000
	iterrate := int32 (((int64(insertions) * 1000000) / us) / 1000) 

	start = time.Now().UnixNano()
	for i := 0; i<insertions; i++ {
		if ! test.Fetch(i) {
			fmt.Println("Failed in Fetch(", i, ")")
			return false,0,0,0
		}
	}
	us = (time.Now().UnixNano() - start) / 1000
	fetchrate := int32 (((int64(insertions) * 1000000) / us) / 1000) 
	return true,insrate,iterrate,fetchrate
}

func TestAutoLinear4(t *testing.T) {
	var test LinearTest	
	test.tree = Create(4)
	pass,_,_,_ := doTest(1000, 40, &test) 
	if (!pass) {
		t.Fail()
	}
}

func TestAutoLinear8(t *testing.T) {
	var test LinearTest	
	test.tree = Create(8)
	pass,_,_,_ := doTest(1000, 1000, &test) 
	if (!pass) {
		t.Fail()
	}
}

func TestAutoLinear16(t *testing.T) {
	var test LinearTest	
	test.tree = Create(16)
	pass,_,_,_ := doTest(1000, 40, &test) 
	if (!pass) {
		t.Fail()
	}
}

func TestRandom4(t *testing.T) {
	insertions := 1000
	deletions := 100
	seed := time.Now().UnixNano()
	test := Generate (4, insertions, deletions, seed)
	t.Log("Random seed:", seed)
	pass,_,_,_ := doTest(insertions, deletions, test)
	if (!pass) {
		t.Fail()
	}
}

func TestRandom8(t *testing.T) {
	insertions := 1000
	deletions := 197
	seed := time.Now().UnixNano()
	test := Generate (8, insertions, deletions, seed)
	t.Log("Random seed:", seed)
	pass,_,_,_ := doTest(insertions, deletions, test)
	if (!pass) {
		t.Fail()
	}
}

func TestRandom16(t *testing.T) {
	insertions := 2000
	deletions := 1999
	seed := time.Now().UnixNano()
	test := Generate (16, insertions, deletions, seed)
	t.Log("Random seed:", seed)
	pass,_,_,_ := doTest(insertions, deletions, test)
	if (!pass) {
		t.Fail()
	}
}