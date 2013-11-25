package btree

import (
//	"fmt"
)

type BPlusTree struct {
	N int
	root node
	head *leaf
}

type node interface {
	add(int, uint64, interface{}) 
	fetch(uint64) interface{}
	remove(int, uint64) interface{}
	split(int) (node, uint64)
	adopt(node, uint64)	
	size() int
}

type pair struct {
	key uint64
	value interface{}		
} 

func (self pair) Key() uint64 {
	return self.key;
}

func (self pair) Value() interface{} {
	return self.value;
}

type leaf struct {
	values [] pair
	neighbor *leaf
}

type inode struct {
	keys [] uint64
	nodes [] node
}

// Initialzie the tree
func NewBPlusTree(order int) (self *BPlusTree) {
	self = new (BPlusTree)
	self.N = order
	self.head = new (leaf)
	self.root = self.head
	return
}

// Insert a key/value
func (self *BPlusTree) Insert(key uint64, value interface{}) {
	self.root.add(self.N, key, value)
	if self.root.size() > self.N {
		newnode, median := self.root.split(self.N)
		newroot := new (inode)
		newroot.keys = append(newroot.keys, median)
		newroot.nodes = append(newroot.nodes, self.root, newnode)
		self.root = newroot
	}
}

// Fetch by key
func (self *BPlusTree) Fetch(key uint64) interface{} {
	return self.root.fetch(key)
}

// Delete value by key
func (self *BPlusTree) Delete(key uint64) {
	self.root.remove(self.N, key)
	if self.root.size() == 0 {
		// reduce (unless the root is a leaf, then do nothing)
		node, ok := self.root.(*inode)
		if (ok) {
			self.root = node.nodes[0]
		}
	}
	return
}

// Iterate over keys
func (self *BPlusTree) Iterate() chan KeyValue {
	ch := make (chan KeyValue)
	go func() {
		for l := self.head; l.neighbor != nil; l = l.neighbor {
			for i := range l.values {
				ch <- l.values[i]
			}
		}
		close (ch)
	}()
	return ch
}

func (self *leaf) find(key uint64) int {
	pos := len(self.values)
	for i, k := range self.values {
		if key < k.key {
			pos = i;
			break
		}
	}
	return pos
}

func (self *leaf) add(N int, key uint64, value interface{}) {
	pos := self.find(key)
	max := len(self.values)
	self.values = append(self.values, pair{key, value})
	if (pos < max) {
		copy(self.values[pos+1:], self.values[pos:])
		self.values[pos] = pair{key, value}
	}
}

func (self *leaf) fetch(key uint64) interface{} {
	pos := self.find(key)
	if pos > 0 && self.values[pos-1].key == key {
		return self.values[pos-1].value
	}
	return nil
}

func (self *leaf) remove(N int, key uint64) interface{} {
	pos := self.find(key)
	if pos > 0 && self.values[pos-1].key == key {
		rval := self.values[pos-1]
		copy(self.values[pos-1:], self.values[pos:])
		self.values = self.values[0:len(self.values)-1]
		return rval
	}	
	return nil
}

func (self *leaf) split(N int) (node, uint64) {
	newleaf := new (leaf)
	newleaf.values = make ([] pair,0,N/2)
	newleaf.values = append(newleaf.values, self.values[N/2:]...)
	self.values = self.values[0:N/2]
	newleaf.neighbor = self.neighbor
	self.neighbor = newleaf
	return newleaf, newleaf.values[0].key
}

func (self *leaf) adopt(other node, median uint64) {
	self.values = append(self.values, other.(*leaf).values...)	
	self.neighbor = other.(*leaf).neighbor
}

func (self *leaf) size() int {
	return len(self.values)
}

func find (keys [] uint64, value uint64) int {
	pos := len(keys)
	for i, k := range keys {
		if value < k {
			pos = i;
			break
		}
	}
	return pos
}

func (self *inode) find(key uint64) int {
	pos := len(self.keys)
	for i, k := range self.keys {
		if key < k {
			pos = i;
			break
		}
	}
	return pos
}

func (self *inode) add(N int, key uint64, value interface{}) {
	pos := self.find(key)
	max := len(self.keys)

	self.nodes[pos].add(N, key, value);
	
	if (self.nodes[pos].size() > N) {
		newnode, median := self.nodes[pos].split(N)
		self.keys = append(self.keys, median)
		self.nodes = append(self.nodes, newnode)
		if (pos < max) {
			copy(self.keys[pos+1:], self.keys[pos:])
			copy(self.nodes[pos+2:], self.nodes[pos+1:])
			self.keys[pos] = median
			self.nodes[pos+1] = newnode
		}
	}
}

func (self *inode) fetch(key uint64) interface{} {
	return self.nodes[self.find(key)].fetch(key)
}

func (self *inode) remove(N int, key uint64) (rval interface{}) {
	pos := self.find(key)
	rval = self.nodes[pos].remove(N, key)
	if self.nodes[pos].size() < N/2	{
		// Underflow, merge
		var left, right int
		if pos == 0 {
			left = 0
			right = 1
		}else{
			left = pos - 1
			right = pos
		}
		self.nodes[left].adopt(self.nodes[right], self.keys[left])
		if self.nodes[left].size() > N {
			// resplit with new median
			self.nodes[right], self.keys[left] = self.nodes[left].split(N)
		}else{
			// reduce (throw away median)
			copy(self.nodes[right:], self.nodes[right+1:])
			self.nodes = self.nodes[0:len(self.nodes)-1]
			copy(self.keys[left:], self.keys[left+1:])
			self.keys = self.keys[0:len(self.keys)-1]
		}
	}
	return
}

func (self *inode) split(N int) (node, uint64) {
	median := self.keys[N/2]	

	newnode := new (inode)
	newnode.keys = make ([] uint64, 0, 0)
	newnode.keys = append(newnode.keys, self.keys[N/2+1:]...)
	self.keys = self.keys[0:N/2]

	newnode.nodes = make ([] node, 0, 0)
	newnode.nodes = append(newnode.nodes, self.nodes[N/2+1:]...)
	self.nodes = self.nodes[0:N/2+1]

	return newnode, median	
}

func (self *inode) adopt(other node, median uint64) {
	self.keys = append(append(self.keys, median), other.(*inode).keys...)
	self.nodes = append(self.nodes, other.(*inode).nodes...)
}

func (self *inode) size() int {
	return len(self.keys)
}
