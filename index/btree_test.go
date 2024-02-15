package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestBTree_Put is a unit test for the Put method of the BTree struct.
func TestBTree_Put(t *testing.T) {
	// Create a new BTree instance.
	bt := NewBTree()

	// Add a key-value pair to the BTree.
	res := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res)
}

// TestBTree_Get is a unit test for the Get method of the BTree struct.
func TestBTree_Get(t *testing.T) {
	// Create a new BTree instance.
	bt := NewBTree()

	// Add a key-value pair to the BTree.
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)

	// Retrieve the value associated with the key from the BTree.
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	// Add another key-value pair to the BTree.
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 101,
	})
	assert.True(t, res2)

	// Retrieve the value associated with the key from the BTree.
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(101), pos2.Offset)

	// Add another key-value pair to the BTree.
	res3 := bt.Put([]byte("b"), &data.LogRecordPos{
		Fid:    3,
		Offset: 102,
	})
	assert.True(t, res3)

	// Retrieve the value associated with the key from the BTree.
	pos3 := bt.Get([]byte("b"))
	assert.Equal(t, uint32(3), pos3.Fid)
	assert.Equal(t, int64(102), pos3.Offset)
}

// TestBTree_Delete is a unit test for the Delete method of the BTree struct.
func TestBTree_Delete(t *testing.T) {
	// Create a new BTree instance.
	bt := NewBTree()

	// Add a key-value pair to the BTree.
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)

	// Delete the key-value pair from the BTree.
	assert.True(t, bt.Delete(nil))

	// Add another key-value pair to the BTree.
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res2)

	// Delete the key-value pair from the BTree.
	assert.True(t, bt.Delete([]byte("a")))
}

// TestBTree_Iterator is a test function for the BTree iterator.
func TestBTree_Iterator(t *testing.T) {
	// Create a new BTree.
	bt1 := NewBTree()

	// BTree is empty.
	iter1 := bt1.Iterator(false)
	assert.Equal(t, iter1.Valid(), false)

	// BTree has one data.
	bt1.Put([]byte("key"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, iter2.Valid(), true)
	iter2.Next()
	assert.Equal(t, iter2.Valid(), false)

	// BTree has multiple data.
	bt1.Put([]byte("key1"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt1.Put([]byte("key2"), &data.LogRecordPos{Fid: 1, Offset: 100})
	bt1.Put([]byte("key3"), &data.LogRecordPos{Fid: 1, Offset: 100})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
		assert.NotNil(t, iter3.Value())
	}
	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
		assert.NotNil(t, iter4.Value())
	}

	// Seek test.
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("key2")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("key2")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
