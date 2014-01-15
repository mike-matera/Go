package btree

import (
//	"fmt"
)

type SimpleNode struct {
	keys [] uint64
	nodes [] * SimpleNode
	values [] int
	neighbor * SimpleNode
}

type SimpleFactory struct {
}

func (self *SimpleFactory) NewNode(order int) Node {
	n := new (SimpleNode)
	n.keys = make ([] uint64, 0, order)
	n.nodes = make ([] *SimpleNode, 0, order)
	n.values = nil
	return n
}

func (self *SimpleFactory) NewLeaf(order int) Node {
	n := new (SimpleNode)
	n.keys = make ([] uint64, 0, order)
	n.values = make ([] int, 0, order)
	n.nodes = nil
	return n
}

func (self *SimpleFactory) Release(n Node) {
}

func (self *SimpleNode) isLeaf() bool {
	return (self.nodes == nil)
}

func (self *SimpleNode) Find(item interface{}) (int, bool, interface{}) { 
	key := item.(uint64)
	pos := len(self.keys)
	for i, k := range self.keys {
		if key < k {
			pos = i
			break
		}
	}
	match := (pos > 0 && key == self.keys[pos-1])
	if self.nodes != nil {
		return pos, match, self.nodes[pos]
	}else{
		if match {
			return pos, match, self.values[pos-1]
		}else{
			return pos, match, nil
		}
	}
}

func (self *SimpleNode) Load(mem * MemNode) {
	if self.nodes != nil {
		for i, k := range self.keys {
			mem.Entries = append(mem.Entries, Entry{k, nil})
			mem.Nodes = append(mem.Nodes, self.nodes[i])
		}
		mem.Nodes = append(mem.Nodes, self.nodes[len(self.keys)])
	}else{
		for i, k := range self.keys {
			mem.Entries = append(mem.Entries, Entry{k, self.values[i]})
		}
		mem.neighbor = self.neighbor
	}
}

func (self *SimpleNode) Store(mem *MemNode) {
	self.keys = nil
	if self.nodes != nil {
		self.nodes = self.nodes[:0]
		for _, k := range mem.Entries {
			self.keys = append(self.keys, k.Key.(uint64))
		}
		for _, n := range mem.Nodes {
			self.nodes = append(self.nodes, n.(*SimpleNode))
		}
	}else{
		self.values = self.values[:0]
		for _, k := range mem.Entries {
			self.keys = append(self.keys, k.Key.(uint64))
			self.values = append(self.values, k.Value.(int))
		}
		self.neighbor = mem.neighbor.(*SimpleNode)
	}
}

func (self *SimpleNode) Dump(c chan Entry) {
	for i, k := range self.keys {
		c <- Entry{k, self.values[i]}
	}
}

func (self *SimpleNode) Next() Node {
	if self.neighbor == nil {
		return nil
	}
	return self.neighbor
}
