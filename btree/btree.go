package btree

import (
//	"fmt"
	"testing"
)

type BTree struct {
	N int
	root * bNode
	Stats struct {
		Size int
		Depth int
		Nodes int
		Leaves int
	}
}

type Pair struct {
	key uint64
	value interface{}
}

func (self Pair) Key() uint64 {
	return self.key
}

func (self Pair) Value() interface{} {
	return self.value
}

type bNode struct {
	Values [] Pair
	Nodes [] *bNode
}

func nodeFind (node *bNode, value uint64) int {
	pos := len(node.Values)
	for i, k := range node.Values {
		if value < k.key {
			pos = i;
			break
		}
	}
	return pos
}

func (tree *BTree) split (self *bNode) (* bNode, Pair) {
	var rnode *bNode 
	median := self.Values[tree.N/2]	

	rnode = new(bNode)
	rnode.Values = make ([] Pair, 0, tree.N+1)
	rnode.Values = append(rnode.Values, self.Values[tree.N/2+1:]...)
	self.Values = self.Values[0:tree.N/2]

	if self.Nodes != nil {
		rnode.Nodes = make ([] * bNode, 0, tree.N+2)
		rnode.Nodes = append(rnode.Nodes, self.Nodes[tree.N/2+1:]...)
		self.Nodes = self.Nodes[0:tree.N/2+1]
		tree.Stats.Nodes++
	}else{
		tree.Stats.Leaves++
	}
		
	//fmt.Print("insert: split: ", self.Values, self.Nodes, "\n")
	//fmt.Print("inster:   and: ", rnode.Values, rnode.Nodes, "\n")

	return rnode, median;
}

func (tree *BTree) valueInsert (pos int, self * bNode, value *Pair, link *bNode) (* bNode, Pair) {
	max := len(self.Values)

	if pos > 0 && self.Values[pos-1].Key() == value.Key() {
		self.Values[pos-1] = *value
	}else{
		self.Values = append (self.Values, *value)
		if (pos < max) {
			copy(self.Values[pos+1:], self.Values[pos:])
			self.Values[pos] = *value
		}
	}
	
	if (link != nil) {
		pos = pos + 1
		self.Nodes = append (self.Nodes, link)
		if (pos < max + 1) {
			copy(self.Nodes[pos+1:], self.Nodes[pos:])
			self.Nodes[pos] = link
		}
	}

	// Split!
	if len(self.Values) == tree.N+1 {
		return tree.split(self)
	}
	
	return nil, Pair{};
}

func (tree * BTree) insert (self *bNode, value *Pair) (*bNode, Pair) {
	var rnode * bNode = nil
	var rval Pair
	
	pos := nodeFind(self, value.key)

	if self.Nodes != nil {
		node, median := tree.insert(self.Nodes[pos], value)
		if node != nil {
			rnode, rval = tree.valueInsert(pos, self, &median, node)
		}
	}else{
		rnode, rval = tree.valueInsert(pos, self, value, nil)
	}
	return rnode, rval
}

func (tree * BTree) Put (index interface{}, value interface{}) {
	node, median := tree.insert(tree.root, &Pair{index.(uint64), value})
	if node != nil {
		n := new(bNode)
		n.Values = make ([] Pair, 1, tree.N+1)
		n.Nodes = make ([] * bNode, 2, tree.N+2)
		n.Values[0] = median
		n.Nodes[0] = tree.root
		n.Nodes[1] = node
		tree.root = n
		tree.Stats.Depth++
	}
	tree.Stats.Size++	
}

func (tree * BTree) fetch (index uint64, node *bNode) (interface{}) {
	pos := nodeFind(node, index)

	if pos > 0 && node.Values[pos-1].key == index {
		return node.Values[pos-1].Value
	}
	
	if node.Nodes != nil {
		return tree.fetch(index, node.Nodes[pos])
	}

	return nil
}

func (tree * BTree) Get (index interface{}) (value interface{}) {
	return tree.fetch(index.(uint64), tree.root)
}

func (tree * BTree) Iterate() chan Entry {
	var spilunk func (n *bNode)
	ch := make (chan Entry)

	spilunk = func (n *bNode) {
		if n.Nodes != nil {
			for i,next := range n.Nodes {
				spilunk(next)
				if i < len(n.Values) {
					ch <- Entry{n.Values[i].Key(), n.Values[i].Value()}
				}
			} 
		}else{
			for _,next := range n.Values {
				ch <- Entry{next.Key(), next.Value()}
			}
		}
	}		
	go func() {
		spilunk(tree.root)
	 	close (ch)
	}()
	return ch
}

func (tree * BTree) balance (parent *bNode, pos int) {

	var left, right int	
	if pos == 0 {
		left = 0 
		right = 1
	}else{
		left = pos-1
		right = pos
	}
	
	// Join neighbors...	
	joined := new (bNode)
	leftnode := parent.Nodes[left]
	rightnode := parent.Nodes[right]
		
	if leftnode.Nodes != nil {
		joined.Nodes = make ([] *bNode, 0, tree.N+2)
		joined.Nodes = append(joined.Nodes, leftnode.Nodes...)
		joined.Nodes = append(joined.Nodes, rightnode.Nodes...)
	}
	joined.Values = make ([] Pair, 0, tree.N+1)
	joined.Values = append(joined.Values, leftnode.Values...)
	joined.Values = append(joined.Values, parent.Values[left])
	joined.Values = append(joined.Values, rightnode.Values...)

	if len(joined.Values) > tree.N {
		// Balance results in two nodes
		parent.Nodes[left] = joined
		parent.Nodes[right], parent.Values[left] = tree.split(joined)
	} else {
		// Balance results in one node
		copy(parent.Values[left:], parent.Values[left+1:])
		parent.Values = parent.Values[0:len(parent.Values)-1]

		copy(parent.Nodes[left:], parent.Nodes[left+1:])
		parent.Nodes = parent.Nodes[0:len(parent.Nodes)-1]
		parent.Nodes[left] = joined

		if joined.Nodes == nil {
			tree.Stats.Leaves--
		}else{
			tree.Stats.Nodes--
		}
	}
}

func (tree * BTree) borrow (node *bNode) (Pair, int) {
	var rvalue Pair
	if node.Nodes != nil {
		// Keep descending
		last := len(node.Nodes) - 1
		borrow, remaining := tree.borrow(node.Nodes[last])
		if remaining < (tree.N/2) {
			// Under threshold, must balance
			tree.balance(node, last)
		}
		rvalue = borrow
	} else{
		// Borrow last value
		last := len(node.Values) - 1	
		rvalue = node.Values[last]
		node.Values = node.Values[0:last]
	}	
	return rvalue, len(node.Values);
}

func (tree * BTree) del (index uint64, node *bNode) int {
	pos := nodeFind(node, index)
	
	if pos > 0 && node.Values[pos-1].key == index {
		// Found the delete value
		tree.Stats.Size--
		if node.Nodes != nil {
			// Lost median, must borrow
			var remaining int
			node.Values[pos-1], remaining = tree.borrow(node.Nodes[pos-1])
			if remaining < (tree.N/2) {
				tree.balance(node, pos-1)
			}
		}else{
			// (leaf node) Kill the entry 
			copy(node.Values[pos-1:], node.Values[pos:])
			node.Values = node.Values[0:len(node.Values)-1]
		}
	} else if node.Nodes != nil {	
		// Value not found... descend
		remaining := tree.del(index, node.Nodes[pos])
		if remaining < (tree.N/2) {
			// Under threshold, must balance
			tree.balance(node, pos)
		}		
	}

	return len(node.Values)
}

func (tree *BTree) Delete(key interface{}) {
	if (tree.del(key.(uint64), tree.root) == 0 && tree.root.Nodes != nil) {
		tree.root = tree.root.Nodes[0]
		tree.Stats.Depth--
	}
}

func NewBTree(order int) *BTree {
	tree := new (BTree)
	tree.N = order
	tree.root = new(bNode)
	tree.root.Values = make ([] Pair, 0, tree.N+1)
	tree.root.Nodes = nil
	tree.Stats.Leaves = 1
	tree.Stats.Nodes = 0
	tree.Stats.Size = 0	
	return tree
}

func (tree *BTree) Check(t *testing.T) {
}
