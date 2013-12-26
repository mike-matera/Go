package btree

import (
	"testing"
)

type NodeFactory interface {
	NewNode() Node
	NewLeaf() Leaf
	Release(TreeNode)
}

type Treelike interface {
	Put(interface{})
	Get(interface{}) interface{}
	Delete(interface{})
	Iterate() chan interface{}
	Check(*testing.T) 
}

type TreeNode interface {
	Size() int
	Find(interface{}) int
	Equals(int, interface{}) bool
	Load(* MemNode) 
	Store(* MemNode)
}

type Leaf interface {
	TreeNode
	Get(interface{}) interface{}
	Dump(chan interface{})
	Next() Leaf
}

type Node interface {
	TreeNode
	Next(interface{}) TreeNode
	GetNode(int) TreeNode
}
