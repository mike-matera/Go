package main 

import (
	"fmt"
	"lifealgorithmic.com/btree"
)

func main() {
	fmt.Println("Hello Go World!")
	tree :=	btree.Create(16)
	tree.Insert(1, 10)
}
