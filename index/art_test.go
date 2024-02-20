package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

// TestNewART is a test function for the NewART function.
func TestNewART(t *testing.T) {
	// Create a new ART instance.
	art := NewART()

	// Put a key-value pair into the ART.
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res1)

	// Put another key-value pair into the ART.
	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res2)

	// Put a third key-value pair into the ART.
	res3 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.NotNil(t, res3)
}

// TestAdaptiveRadixTree_Get is a test function for the Get method of the AdaptiveRadixTree struct.
func TestAdaptiveRadixTree_Get(t *testing.T) {
	// Create a new AdaptiveRadixTree object
	art := NewART()

	// Put a key-value pair into the tree
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})

	// Get the value associated with the key "key-1"
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)
	// Get the value associated with the key "not exist"
	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	// Put a new key-value pair into the tree
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1123, Offset: 990})

	// Get the updated value associated with the key "key-1"
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

// TestAdaptiveRadixTree_Delete is a test function for the Delete method of the AdaptiveRadixTree.
func TestAdaptiveRadixTree_Delete(t *testing.T) {
	// Create a new ART instance.
	art := NewART()

	// Delete a non-existent key from the ART.
	res1 := art.Delete([]byte("not exist"))
	assert.False(t, res1)

	// Put a key-value pair into the ART.
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})

	// Delete the key from the ART.
	res2 := art.Delete([]byte("key-1"))
	assert.True(t, res2)

	// Get the position of the deleted key from the ART.
	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

// TestAdaptiveRadixTree_Size tests the Size method of AdaptiveRadixTree.
func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	// Check if the size of the tree is 0.
	assert.Equal(t, 0, art.Size())

	// Add some key-value pairs to the tree.
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})

	// Check if the size of the tree is 2.
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
