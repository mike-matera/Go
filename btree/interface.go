package btree

import (
)

type Treelike interface {
	Insert(uint64, interface{})
	Fetch(uint64) interface{}
	Delete(uint64)
	Iterate() chan KeyValue
}

type KeyValue interface {
	Key() uint64
	Value() interface{}
}

