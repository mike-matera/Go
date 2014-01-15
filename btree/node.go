package btree

import (
	"testing"
)

type NodeFactory interface {
	NewNode(order int) Node
	NewLeaf(order int) Node
	Release(Node)
}

type Entry struct {
	Key interface{}
	Value interface{}
}

type Treelike interface {
	Put(interface{}, interface{})
	Get(interface{}) interface{}
	Delete(interface{})
	Iterate() chan Entry
	Check(*testing.T) 
}

type Node interface {
	isLeaf() bool
	Find(interface{}) (int, bool, interface{})
	Load(* MemNode) 
	Store(* MemNode)
	Dump(chan Entry)
	Next() Node
}
