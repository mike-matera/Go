package btree

import (
//	"fmt"
)

type SimpleNode struct {
	keys [] uint64
	nodes [] TreeNode
}

type SimpleLeaf struct {
	values [] Pair
	neighbor *SimpleLeaf
}

type Pair struct {
	Key uint64
	Value int
}

type SimpleFactory struct {
}

func (self *SimpleFactory) NewNode() Node {
	return new (SimpleNode)
}

func (self *SimpleFactory) NewLeaf() Leaf {
	return new (SimpleLeaf)
}

func (self *SimpleFactory) Release(n TreeNode) {
}

func (self *SimpleLeaf) Size() int {
	return len(self.values)
}

func (self *SimpleLeaf) Find(item interface{}) int { 
	key := item.(Pair).Key
	for i, k := range self.values {
		if key < k.Key {
			return i	
		}
	}
	return len(self.values)
}

func (self *SimpleLeaf) Equals(index int, item interface{}) bool {
	return (item.(Pair).Key == self.values[index].Key)
}

func (self *SimpleLeaf) Get(item interface{}) interface{} {
	key := item.(Pair).Key
	for _, k := range self.values {
		if key == k.Key {
			return k
		}
	}
	return nil	
}

func (self *SimpleLeaf) Load(mem * MemNode) {
	for i, k := range self.values {
		mem.Keys[i] = k
	}
	mem.neighbor = self.neighbor
}

func (self *SimpleLeaf) Store(mem *MemNode) {
	self.values = nil
	for _, k := range mem.Keys {
		self.values = append(self.values, k.(Pair))
	}
	self.neighbor = mem.neighbor.(*SimpleLeaf)
}

func (self *SimpleLeaf) Dump(c chan interface{}) {
	for _, k := range self.values {
		c <- k 
	}
}

func (self *SimpleLeaf) Next() Leaf {
	if self.neighbor == nil {
		return nil
	} 
	return self.neighbor
}

func (self *SimpleNode) Size() int {
	return len(self.keys)
}

func (self *SimpleNode) Find(item interface{}) int { 
	key := item.(Pair).Key
	for i, k := range self.keys {
		if key < k {
			return i	
		}
	}
	return len(self.keys)
}

func (self *SimpleNode) Equals(index int, item interface{}) bool {
	return (item.(uint64) == self.keys[index])
}

func (self *SimpleNode) Next(item interface{}) TreeNode {
	key := item.(Pair).Key
	for i, k := range self.keys {
		if key < k {
			return self.nodes[i]
		}
	}
	return self.nodes[len(self.keys)]
}

func (self *SimpleNode) GetNode(item int) TreeNode {
	return self.nodes[item]
}

func (self *SimpleNode) Load(mem * MemNode) {
	for i, k := range self.keys {
		mem.Keys[i] = k
		mem.Nodes[i] = self.nodes[i]
	}
	mem.Nodes[len(self.keys)] = self.nodes[len(self.keys)]
}

func (self *SimpleNode) Store(mem * MemNode) {
	self.keys = nil
	self.nodes = nil
	for _, k := range mem.Keys {
		switch v := k.(type) {
			case uint64:
				self.keys = append(self.keys, v)
			case Pair:
				self.keys = append(self.keys, v.Key)
		}
	}
	for _, k := range mem.Nodes {
		self.nodes = append(self.nodes, k.(TreeNode))
	}
}
