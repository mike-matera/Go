package btree

import (
//	"fmt"
	"testing"
)

type BPlusTree struct {
	N int
	root TreeNode
	factory SimpleFactory
	head Leaf
}

type MemNode struct {
	ref TreeNode
	neighbor TreeNode
	Keys [] interface{}
	Nodes [] interface{}
}

// Initialzie the tree
func NewBPlusTree(order int) (self *BPlusTree) {
	self = new (BPlusTree)
	self.N = order
	self.root = self.factory.NewLeaf()
	self.head = self.root.(Leaf)
	return
}

// Fetch by key
func (self *BPlusTree) Get(key interface{}) interface{} {
	//fmt.Println("Get(" , key, ")")
	var recurse func (TreeNode) interface{}
	recurse = func(n TreeNode) (rval interface{}) {
		switch node := n.(type) {
			case Leaf:
				rval = node.Get(key)

			case Node:
				rval = recurse(node.Next(key))
		} 
		return
	}
	return recurse(self.root)
}


func (self * BPlusTree) load(t TreeNode) * MemNode {
	rval := new (MemNode)
	size := t.Size()
	rval.Keys = make ([] interface{}, size)
	switch t.(type) {
		case Node:
			rval.Nodes = make([] interface{}, size+1)
	}
	rval.ref = t
	t.Load(rval)
	return rval
}

func (self * BPlusTree) store(m *MemNode) {
	m.ref.Store(m)
}

// Insert a key/value
func (self *BPlusTree) Put(item interface{}) {
	//fmt.Println("Put(" , item, ")")
	var recurse func(TreeNode) (* MemNode, interface{})
	recurse = func (n TreeNode) (* MemNode, interface{}) {
		pos := n.Find(item)
		var insert_item interface{} 
		var insert_node * MemNode
		
		switch node := n.(type) {
			case Leaf:
				insert_item = item
			case Node:
				insert_node, insert_item = recurse(node.Next(item))
		}

		if insert_item != nil {
			temp := self.load(n)
			defer self.store(temp)
			self.insert(pos, temp, insert_item, insert_node)
			if len(temp.Keys) > self.N {
				return self.split(temp)
			}
		}
		return nil, nil
	}
	
	node, median := recurse(self.root)
	if node != nil {
		newroot := new (MemNode)
		newroot.ref = self.factory.NewNode()
		newroot.Keys = make([] interface{}, 1, 1)
		newroot.Nodes = make([] interface{}, 2, 2)
		newroot.Keys[0] = median
		newroot.Nodes[0] = self.root
		newroot.Nodes[1] = node.ref
		self.store(newroot)
		self.root = newroot.ref		
	}
}

func (self *BPlusTree) Delete(key interface{}) {
	//fmt.Println("Delete(" , key, ")")
	var del func (n TreeNode) * MemNode
	del = func(n TreeNode) (temp * MemNode) {
		//fmt.Println("(del): <", key, ">", n)
		pos := n.Find(key)
		switch node := n.(type) {
			case Leaf:
				// (leaf node) Kill the entry 
				if pos > 0 && node.Equals(pos-1, key) {
					temp = self.load(n)
					copy(temp.Keys[pos-1:], temp.Keys[pos:])
					temp.Keys = temp.Keys[0:len(temp.Keys)-1]
				}

			case Node:
				updated := del(node.Next(key))
				if updated != nil {
					if len(updated.Keys) < self.N/2 {
						temp = self.pivot(pos, updated, node)
					}else{
						self.store(updated)
					}
				}
		}
		//fmt.Println("(del/done): <", key, ">", temp)
		return 
	}
	modified := del(self.root)
	if modified != nil {
		// Root was modified
		if modified.Nodes != nil {
			if len(modified.Nodes) == 1 {
				self.root = modified.Nodes[0].(TreeNode)
				self.factory.Release(modified.ref)
			}else{
				self.store(modified)
			}
		}else{
			self.store(modified)
		}
	}
}

func (self *BPlusTree) pivot(pos int, child *MemNode, node Node) * MemNode {
	root := self.load(node)
	left := 0; right := 1
	var leftnode, rightnode *MemNode 
	if pos > 0 {
		left = pos - 1
		right = pos
		rightnode = child
		leftnode = self.load(node.GetNode(left))
	}else{
		leftnode = child
		rightnode = self.load(node.GetNode(right))
	}

	// Join neighbors...	
	joined := new (MemNode)
	joined.ref = leftnode.ref
	joined.neighbor = rightnode.neighbor
			
	if leftnode.Nodes != nil {
		// Node join
		joined.Nodes = make ([] interface{}, 0, self.N+1)
		joined.Nodes = append(joined.Nodes, leftnode.Nodes...)
		joined.Nodes = append(joined.Nodes, rightnode.Nodes...)
		joined.Keys = make ([] interface{}, 0, self.N)
		joined.Keys = append(joined.Keys, leftnode.Keys...)
		joined.Keys = append(joined.Keys, root.Keys[left])
		joined.Keys = append(joined.Keys, rightnode.Keys...)
	}else{
		// Leaf join
		joined.Keys = make ([] interface{}, 0, self.N)
		joined.Keys = append(joined.Keys, leftnode.Keys...)
		joined.Keys = append(joined.Keys, rightnode.Keys...)
	}

	if len(joined.Keys) > self.N {
		// Pivot results in two nodes
		root.Nodes[left] = joined.ref
		newnode, newmedian := self.split(joined)
		root.Nodes[right] = newnode.ref
		root.Keys[left] = newmedian
				
		self.store(joined)
		self.store(rightnode)

	} else {
		// Pivot results in one node
		copy(root.Keys[left:], root.Keys[left+1:])
		root.Keys = root.Keys[0:len(root.Keys)-1]

		copy(root.Nodes[left:], root.Nodes[left+1:])
		root.Nodes = root.Nodes[0:len(root.Nodes)-1]
		root.Nodes[left] = joined.ref
		
		self.store(joined)
		self.factory.Release(rightnode.ref)		
	}
	
	return root
}

func (self *BPlusTree) Check(t *testing.T)  {
	/*
	var checker func (n TreeNode)
	checker = func (n TreeNode) {
		switch node := n.(type) {
			case Node:
				temp := self.load(node)
				for _,k := range temp.Nodes {
					checker(k.(TreeNode))
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
	return
}

func (self *BPlusTree) Iterate() chan interface{} {
	//fmt.Println("Iterate()")
	ch := make (chan interface{})
	
	go func() {
		for working := self.head; working != nil; working = working.Next() {
			working.Dump(ch)
		}
	 	close (ch)
	}()
	return ch
}

func (self *BPlusTree) insert (pos int, node *MemNode, value interface{}, link *MemNode) {
	max := len(node.Keys)
	node.Keys = append(node.Keys, value)
	if (pos < max) {
		copy(node.Keys[pos+1:], node.Keys[pos:])
		node.Keys[pos] = value
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

func (self *BPlusTree) split (node *MemNode) (*MemNode, interface{}) {
	var rnode *MemNode 
	median := node.Keys[self.N/2]

	rnode = new(MemNode)
	switch node.ref.(type) {
		case Leaf:
			rnode.ref = self.factory.NewLeaf()

			rnode.Keys = make ([] interface{}, 0, self.N)	
			rnode.Keys = append(rnode.Keys, node.Keys[self.N/2:]...)
			node.Keys = node.Keys[0:self.N/2]

			rnode.neighbor = node.neighbor
			node.neighbor = rnode.ref

		case Node:
			rnode.ref = self.factory.NewNode()

			rnode.Keys = make ([] interface{}, 0, self.N)	
			rnode.Keys = append(rnode.Keys, node.Keys[self.N/2+1:]...)
			node.Keys = node.Keys[0:self.N/2]

			rnode.Nodes = make ([] interface{}, 0, self.N+2)
			rnode.Nodes = append(rnode.Nodes, node.Nodes[self.N/2+1:]...)
			node.Nodes = node.Nodes[0:self.N/2+1]
	}
	
	self.store(rnode)	
	return rnode, median;
}
