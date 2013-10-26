GoLibrary
=========

Go code projects that I'm implementing so that I can learn Go. I'm focusing mostly on algorithms. You may
find them useful. In the library you will find:

lifealgorithmic.com/btree - An implementation of a B-tree indexed container
  The B-tree is a simple container implementation with the fllowing methods
  
    Create(order int) (Btree *)
      Return a B-tree container with nodes that hold order values (must be an even number)
      
    (Btree *) Insert(index uint64, interface{}) 
      Insert a value with the specified index
      
    (Btree *) Delete(index uint64)
      Delete the value with the specified index
      
    (Btree *) Fetch(index uint64) (interface{})
      Fetch the value with the specified index. Returns nil if the index was not present
      
    (Btree *) Iterate() (chan uint64) 
      Returns a channel that sources all of the indexes in the B-tree
      
