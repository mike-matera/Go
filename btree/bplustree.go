package btree

import (
//	"fmt"
	"testing"
)

type BPlusTree struct {
	N int
	root Node
	factory SimpleFactory
	head Node
}

type MemNode struct {
	ref Node
	neighbor Node
	Entries [] Entry
	Nodes [] Node
}

// Initialzie the tree
func NewBPlusTree(order int) (self *BPlusTree) {
	self = new (BPlusTree)
	self.N = order
	self.root = self.factory.NewLeaf()
	self.head = self.root
	return
}

// Fetch by key
func (self *BPlusTree) Get(key interface{}) interface{} {
	var recurse func (Node) interface{}
	recurse = func(n Node) (interface{}) {
		_, _, next := n.Find(key)
		if n.isLeaf() {
			return next
		}
		return recurse(next.(Node))
	}
	return recurse(self.root)
}

// Insert a key/value
func (self *BPlusTree) Put(key interface{}, value interface{}) {
	var recurse func(Node) (* MemNode, *Entry)
	recurse = func (n Node) (* MemNode, *Entry) {
		pos, _, next := n.Find(key)
		var temp_value * Entry = nil
		var temp_node * MemNode = nil
		
		if n.isLeaf() {
			temp_value = &Entry{key, value}
		}else{
			temp_node, temp_value = recurse(next.(Node))
		}

		if temp_value != nil {
			me := self.load(n)
			defer self.store(me)
			self.insert(pos, me, temp_value, temp_node)
			if len(me.Entries) > self.N {
				return self.split(me)
			}
		}
		
		return nil, nil
	}	
	node, median := recurse(self.root)
	if node != nil {
		newroot := new (MemNode)
		newroot.ref = self.factory.NewNode()
		newroot.Entries = make([] Entry, 1, 1)
		newroot.Nodes = make([] Node, 2, 2)
		newroot.Entries[0] = *median
		newroot.Nodes[0] = self.root
		newroot.Nodes[1] = node.ref
		self.store(newroot)
		self.root = newroot.ref		
	}
}

func (self *BPlusTree) Delete(key interface{}) {
	var del func (n Node) * MemNode
	del = func(n Node) (temp * MemNode) {
		pos, match, next := n.Find(key)
		if n.isLeaf() {
			// (leaf node) Kill the entry 
			if match {
				temp = self.load(n)
				copy(temp.Entries[pos-1:], temp.Entries[pos:])
				temp.Entries = temp.Entries[0:len(temp.Entries)-1]
			}
		}else{
			updated := del(next.(Node))
			if updated != nil {
				if len(updated.Entries) < self.N/2 {
					temp = self.load(n)
					self.pivot(pos, updated, temp)
				}else{
					self.store(updated)
				}
			}
		}
		return 
	}
	modified := del(self.root)
	if modified != nil {
		// Root was modified
		if modified.Nodes != nil {
			if len(modified.Nodes) == 1 {
				self.root = modified.Nodes[0].(Node)
				self.factory.Release(modified.ref)
			}else{
				self.store(modified)
			}
		}else{
			self.store(modified)
		}
	}
}

func (self *BPlusTree) Check(t *testing.T)  {
/*
	//fmt.Println("-- BEGIN TREE CHECK --")
	var checker func (n Node)
	checker = func (n Node) {
		//fmt.Println("check:", n)
		if ! n.isLeaf() {
			temp := self.load(n)
			for _,k := range temp.Nodes {
				checker(k)
			}
		}
		size := n.Size()
		if (size < self.N/2 && n != self.root) || size > self.N {
			t.Error("Check: Node size:", size, n)
			t.FailNow()
		}
	}
	checker(self.root)
*/
}

func (self *BPlusTree) Iterate() chan Entry {
	ch := make (chan Entry)
	
	go func() {
		for working := self.head; working != nil; working = working.Next() {
			working.Dump(ch)
		}
	 	close (ch)
	}()
	return ch
}

func (self *BPlusTree) insert (pos int, node *MemNode, value *Entry, link *MemNode) {
	max := len(node.Entries)
	node.Entries = append(node.Entries, *value)
	if (pos < max) {
		copy(node.Entries[pos+1:], node.Entries[pos:])
		node.Entries[pos] = *value
	}

	if (link != nil) {
		pos = pos + 1
		node.Nodes = append (node.Nodes, link.ref)
		if (pos < max + 1) {
			copy(node.Nodes[pos+1:], node.Nodes[pos:])
			node.Nodes[pos] = link.ref
		}
	}
}

func (self *BPlusTree) split (node *MemNode) (*MemNode, *Entry) {
	var rnode *MemNode 
	median := node.Entries[self.N/2]

	rnode = new(MemNode)
	if node.ref.isLeaf() {
		rnode.ref = self.factory.NewLeaf()

		rnode.Entries = make ([] Entry, 0, self.N)	
		rnode.Entries = append(rnode.Entries, node.Entries[self.N/2:]...)
		node.Entries = node.Entries[0:self.N/2]

		rnode.neighbor = node.neighbor
		node.neighbor = rnode.ref
	}else{
		rnode.ref = self.factory.NewNode()

		rnode.Entries = make ([] Entry, 0, self.N)	
		rnode.Entries = append(rnode.Entries, node.Entries[self.N/2+1:]...)
		node.Entries = node.Entries[0:self.N/2]

		rnode.Nodes = make ([] Node, 0, self.N+2)
		rnode.Nodes = append(rnode.Nodes, node.Nodes[self.N/2+1:]...)
		node.Nodes = node.Nodes[0:self.N/2+1]
	}

	self.store(rnode)	
	return rnode, &median;
}

func (self *BPlusTree) pivot(pos int, child *MemNode, root *MemNode) {
	left := 0; right := 1
	var leftnode, rightnode *MemNode 
	if pos > 0 {
		left = pos - 1
		right = pos
		rightnode = child
		leftnode = self.load(root.Nodes[left])
	}else{
		leftnode = child
		rightnode = self.load(root.Nodes[right])
	}

	// Join neighbors...	
	joined := new (MemNode)
	joined.ref = leftnode.ref
	joined.neighbor = rightnode.neighbor
			
	if leftnode.Nodes != nil {
		// Node join
		joined.Nodes = make ([] Node, 0, self.N+1)
		joined.Nodes = append(joined.Nodes, leftnode.Nodes...)
		joined.Nodes = append(joined.Nodes, rightnode.Nodes...)
		joined.Entries = make ([] Entry, 0, self.N)
		joined.Entries = append(joined.Entries, leftnode.Entries...)
		joined.Entries = append(joined.Entries, root.Entries[left])
		joined.Entries = append(joined.Entries, rightnode.Entries...)
	}else{
		// Leaf join
		joined.Entries = make ([] Entry, 0, self.N)
		joined.Entries = append(joined.Entries, leftnode.Entries...)
		joined.Entries = append(joined.Entries, rightnode.Entries...)
	}

	if len(joined.Entries) > self.N {
		// Pivot results in two nodes
		root.Nodes[left] = joined.ref
		newnode, newmedian := self.split(joined)
		root.Nodes[right] = newnode.ref
		root.Entries[left] = *newmedian
				
		self.store(joined)
		self.store(rightnode)

	} else {
		// Pivot results in one node
		copy(root.Entries[left:], root.Entries[left+1:])
		root.Entries = root.Entries[0:len(root.Entries)-1]

		copy(root.Nodes[left:], root.Nodes[left+1:])
		root.Nodes = root.Nodes[0:len(root.Nodes)-1]
		root.Nodes[left] = joined.ref
		
		self.store(joined)
		self.factory.Release(rightnode.ref)		
	}
}

func (self * BPlusTree) load(t Node) * MemNode {
	rval := new (MemNode)
	rval.Entries = make ([] Entry, 0, self.N)
	if ! t.isLeaf() {
		rval.Nodes = make([] Node, 0, self.N+1)
	}
	rval.ref = t
	t.Load(rval)
	return rval
}

func (self * BPlusTree) store(m *MemNode) {
	m.ref.Store(m)
}
