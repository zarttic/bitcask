package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// TestDB_NewIterator is a test function for the NewIterator method of the DB struct.
func TestDB_NewIterator(t *testing.T) {
	// Create a new configuration for the DB.
	cfg := DefaultConfig
	// Create a temporary directory for testing.
	dir, err := os.MkdirTemp("", "bitcask-test-iterator")
	assert.Nil(t, err)
	cfg.DirPath = dir

	// Open a new DB instance with the default configuration.
	db, err := Open(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Create a new iterator with the default configuration.
	iterator := db.NewIterator(DefaultIteratorConfig)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

// TestDB_NewIterator_One_Value is a test function for the NewIterator method of the DB struct.
// It tests the scenario where the iterator is created with a single value.
func TestDB_NewIterator_One_Value(t *testing.T) {
	// Create a new configuration for the DB.
	cfg := DefaultConfig
	// Create a temporary directory for testing.
	dir, err := os.MkdirTemp("", "bitcask-test-iterator-one-value")
	assert.Nil(t, err)
	cfg.DirPath = dir

	// Open a new DB instance with the default configuration.
	db, err := Open(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Put a test key-value pair into the DB.
	key := utils.GetTestKey(10)
	value := utils.GetTestValue(10)
	err = db.Put(key, value)
	assert.Nil(t, err)

	// Create a new iterator with the default configuration.
	iterator := db.NewIterator(DefaultIteratorConfig)
	assert.NotNil(t, iterator)

	// Verify that the iterator is valid and has the correct key.
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, key, iterator.Key())

	// Verify that the iterator has the correct value.
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, value, val)
}

// TestDB_NewIterator_Multi_Values is a test function for the NewIterator method of the DB struct.
func TestDB_NewIterator_Multi_Values(t *testing.T) {
	// Create a new configuration for the DB.
	cfg := DefaultConfig
	// Create a temporary directory for the DB.
	dir, err := os.MkdirTemp("", "bitcask-test-iterator-multi-values")
	assert.Nil(t, err)
	cfg.DirPath = dir
	// Open a new DB instance.
	db, err := Open(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// Create a key and value for testing.
	key := utils.GetTestKey(10)
	value := utils.GetTestValue(10)
	// Put the key-value pair into the DB.
	err = db.Put(key, value)
	assert.Nil(t, err)
	// Put multiple key-value pairs into the DB.
	err = db.Put([]byte("a"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("b"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("c"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("d"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("e"), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ae"), utils.GetTestValue(10))
	assert.Nil(t, err)
	// Create a new iterator for the DB.
	it1 := db.NewIterator(DefaultIteratorConfig)
	assert.NotNil(t, it1)
	// Iterate over all key-value pairs in the DB.
	for it1.Rewind(); it1.Valid(); it1.Next() {
		assert.NotNil(t, it1.Key())
	}
	// Rewind the iterator to the beginning.
	it1.Rewind()
	// Iterate over all key-value pairs starting from "c" in the DB.
	for it1.Seek([]byte("c")); it1.Valid(); it1.Next() {
		assert.NotNil(t, it1.Key())
	}

	// Reverse iteration
	it2 := db.NewIterator(IteratorConfig{Reverse: true})
	assert.NotNil(t, it2)
	// Iterate over all key-value pairs in the DB in reverse order.
	for it2.Rewind(); it2.Valid(); it2.Next() {
		assert.NotNil(t, it2.Key())
	}
	// Rewind the iterator to the beginning.
	it2.Rewind()
	// Iterate over all key-value pairs starting from "c" in the DB in reverse order.
	for it2.Seek([]byte("c")); it2.Valid(); it2.Next() {
		assert.NotNil(t, it2.Key())
	}

	// Iterate over all key-value pairs starting from "a" in the DB.
	it3 := db.NewIterator(IteratorConfig{Prefix: []byte("a")})
	assert.NotNil(t, it3)
	for it3.Rewind(); it3.Valid(); it3.Next() {
		assert.NotNil(t, it3.Key())
	}
}
