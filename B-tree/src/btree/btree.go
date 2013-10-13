package btree

import (
//	"fmt"
)

var N int = 16

type Btree struct {
	root * Node; 
}

type Pair struct {
	Key uint64
	Value interface{}
}

type Node struct {
	Values [] Pair
	Nodes [] *Node
}

func newRootNode (a *Node, b *Node, median *Pair) * Node {
	n := new(Node)
	n.Values = make ([] Pair, 1, N+1)
	n.Nodes = make ([] * Node, 2, N+2)
	n.Values[0] = *median;
	n.Nodes[0] = a;
	n.Nodes[1] = b;
	return n;
}

func newNode () * Node {
	n := new(Node)
	n.Values = make ([] Pair, 0, N+1)
	n.Nodes = make ([] * Node, 0, N+2)
	return n;
}

func newLeaf () * Node {
	n := new(Node)
	n.Values = make ([] Pair, 0, N+1)
	n.Nodes = nil;
	return n;
}

func nodeFind (node *Node, value *Pair) int {
	pos := len(node.Values)
	for i, k := range node.Values {
		if value.Key < k.Key {
			pos = i;
			break
		}
	}
	return pos
}

func (self * Node) valueInsert (value *Pair, link *Node) (* Node, Pair) {
	//fmt.Printf("\ninsert: value: %d [%p]\n", value.Key, link)
	//fmt.Print("insert:    is: ", self.Values, self.Nodes, "\n")

	// Find the place
	pos := nodeFind(self, value)
	max := len(self.Values)

	self.Values = append (self.Values, *value)
	if (pos < max) {
		copy(self.Values[pos+1:], self.Values[pos:])
		self.Values[pos] = *value
	}

	if (link != nil) {
		pos = pos + 1
		self.Nodes = append (self.Nodes, link)
		if (pos < max + 1) {
			copy(self.Nodes[pos+1:], self.Nodes[pos:])
			self.Nodes[pos] = link
		}
	}

	//fmt.Print("insert:   now: ", self.Values, self.Nodes, "\n")
	
	// Split!
	if len(self.Values) == N+1 {
		var rnode *Node 
		median := self.Values[N/2]	

		if self.Nodes != nil {
			rnode = newNode()
			rnode.Nodes = append(rnode.Nodes, self.Nodes[N/2+1:]...)
			self.Nodes = self.Nodes[0:N/2+1]
		}else{
		 	rnode = newLeaf()
		}
		
		rnode.Values = append(rnode.Values, self.Values[N/2+1:]...)
		self.Values = self.Values[0:N/2]

		//fmt.Print("insert: split: ", self.Values, self.Nodes, "\n")
		//fmt.Print("inster:   and: ", rnode.Values, rnode.Nodes, "\n")

		return rnode, median;
	}
	
	return nil, Pair{};
}

func (self * Node) insert (value *Pair) (*Node, Pair) {
	var rnode * Node = nil
	var rval Pair
	
	if self.Nodes != nil {
		pos := nodeFind(self, value)
		node, median := self.Nodes[pos].insert(value)
		if node != nil {
			rnode, rval = self.valueInsert(&median, node)
		}
	}else{
		rnode, rval = self.valueInsert(value, nil)
	}
	
	return rnode, rval
}

func spilunk (n *Node, ch chan uint64) {
	if n.Nodes != nil {
		for i,next := range n.Nodes {
			spilunk(next, ch)
			if i < len(n.Values) {
				ch <- n.Values[i].Key
			}
		} 
	}else{
		for _,next := range n.Values {
			ch <- next.Key
		}
	}		
}

func (self * Btree) Insert (index uint64, value *interface{}) {
	if self.root == nil {
		self.root = newLeaf()
	}
	node, median := self.root.insert(&Pair{index, value})
	if node != nil {
		self.root = newRootNode(self.root, node, &median)
	}
}

func (self * Btree) Iterate() chan uint64 {
	ch := make (chan uint64)
	go func() {
	 spilunk(self.root, ch)
	 close (ch)
	}()
	return ch
}

