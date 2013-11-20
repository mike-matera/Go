package btree

import (
//	"fmt"
)

type Btree struct {
	N int
	root * Node
	Stats struct {
		Size int
		Depth int
		Nodes int
		Leaves int
	}
}

type Pair struct {
	Key uint64
	Value interface{}
}

type Node struct {
	Values [] Pair
	Nodes [] *Node
}

func nodeFind (node *Node, value uint64) int {
	pos := len(node.Values)
	for i, k := range node.Values {
		if value < k.Key {
			pos = i;
			break
		}
	}
	return pos
}

func (tree *Btree) split (self *Node) (* Node, Pair) {
	var rnode *Node 
	median := self.Values[tree.N/2]	

	rnode = new(Node)
	rnode.Values = make ([] Pair, 0, tree.N+1)
	rnode.Values = append(rnode.Values, self.Values[tree.N/2+1:]...)
	self.Values = self.Values[0:tree.N/2]

	if self.Nodes != nil {
		rnode.Nodes = make ([] * Node, 0, tree.N+2)
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

func (tree *Btree) valueInsert (pos int, self * Node, value *Pair, link *Node) (* Node, Pair) {
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

	// Split!
	if len(self.Values) == tree.N+1 {
		return tree.split(self)
	}
	
	return nil, Pair{};
}

func (tree * Btree) insert (self *Node, value *Pair) (*Node, Pair) {
	var rnode * Node = nil
	var rval Pair
	
	pos := nodeFind(self, value.Key)

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

func (tree * Btree) Insert (index uint64, value interface{}) {
	node, median := tree.insert(tree.root, &Pair{index, value})
	if node != nil {
		n := new(Node)
		n.Values = make ([] Pair, 1, tree.N+1)
		n.Nodes = make ([] * Node, 2, tree.N+2)
		n.Values[0] = median
		n.Nodes[0] = tree.root
		n.Nodes[1] = node
		tree.root = n
		tree.Stats.Depth++
	}
	tree.Stats.Size++	
}

func (tree * Btree) fetch (index uint64, node *Node) (interface{}) {
	pos := nodeFind(node, index)

	if pos > 0 && node.Values[pos-1].Key == index {
		return node.Values[pos-1].Value
	}
	
	if node.Nodes != nil {
		return tree.fetch(index, node.Nodes[pos])
	}

	return nil
}

func (tree * Btree) Fetch (index uint64) (value interface{}) {
	return tree.fetch(index, tree.root)
}

func (tree * Btree) Iterate() chan uint64 {
	var spilunk func (n *Node)
	ch := make (chan uint64)

	spilunk = func (n *Node) {
		if n.Nodes != nil {
			for i,next := range n.Nodes {
				spilunk(next)
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
	go func() {
		spilunk(tree.root)
	 	close (ch)
	}()
	return ch
}

func (tree * Btree) balance (parent *Node, pos int) {

	var left, right int	
	if pos == 0 {
		left = 0 
		right = 1
	}else{
		left = pos-1
		right = pos
	}
	
	// Join neighbors...	
	joined := new (Node)
	leftnode := parent.Nodes[left]
	rightnode := parent.Nodes[right]
		
	if leftnode.Nodes != nil {
		joined.Nodes = make ([] *Node, 0, tree.N+2)
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

func (tree * Btree) borrow (node *Node) (Pair, int) {
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

func (tree * Btree) del (index uint64, node *Node) int {
	pos := nodeFind(node, index)
	
	if pos > 0 && node.Values[pos-1].Key == index {
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

func (tree *Btree) Delete(key uint64) {
	if (tree.del(key, tree.root) == 0 && tree.root.Nodes != nil) {
		tree.root = tree.root.Nodes[0]
		tree.Stats.Depth--
	}
}

func Create(order int) * Btree {
	tree := new (Btree)
	tree.N = order
	tree.root = new(Node)
	tree.root.Values = make ([] Pair, 0, tree.N+1)
	tree.root.Nodes = nil;
	tree.Stats.Leaves = 1
	tree.Stats.Nodes = 0
	tree.Stats.Size = 0	
	return tree	
}
