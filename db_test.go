package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.cfg.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

// TestOpen is a test function for the Open function.
func TestOpen(t *testing.T) {
	cfg := DefaultConfig
	// Create a temporary directory for testing.
	temp, err := os.MkdirTemp("", "bitcask-test-open")

	assert.Nil(t, err)
	cfg.DirPath = temp

	// Open a new Bitcask database.
	db, err := Open(cfg)

	assert.Nil(t, err)
	assert.NotNil(t, db)

}

// TestDB_Put is a unit test function for the Put method of the DB struct.
//func TestDB_Put(t *testing.T) {
//	// Create a temporary directory for testing
//	cfg := DefaultConfig
//	cfg.DataFileSize = 64 * 1024 * 1024
//	temp, err := os.MkdirTemp("", "bitcask-test-put")
//	assert.Nil(t, err)
//	cfg.DirPath = temp
//
//	// Open a new DB instance
//	db, err := Open(cfg)
//
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	// Test putting a new key-value pair
//	key := utils.GetTestKey(1)
//	val := utils.GetTestValue(24)
//	err = db.Put(key, val)
//	assert.Nil(t, err)
//	val1, err := db.Get(key)
//	assert.Nil(t, err)
//	assert.Equal(t, val, val1)
//
//	// Test putting an existing key-value pair
//	val = utils.GetTestValue(24)
//	err = db.Put(key, val)
//	assert.Nil(t, err)
//	val2, err := db.Get(key)
//	assert.Nil(t, err)
//	assert.Equal(t, val, val2)
//
//	// Test putting a key with an empty key
//	err = db.Put(nil, val)
//	assert.Equal(t, err, ErrKeyIsEmpty)
//
//	// Test putting a value with an empty value
//	err = db.Put(key, nil)
//	assert.Nil(t, err)
//	val3, err := db.Get(key)
//	assert.Nil(t, err)
//	assert.Equal(t, len(val3), 0)
//	//TODO removed this test
//	////Test putting a large number of key-value pairs
//	//for i := 0; i < 1000000; i++ {
//	//	err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
//	//	assert.Nil(t, err)
//	//}
//	//// Restart the DB instance and test the Put method again
//	//assert.Equal(t, 2, len(db.oldFile))
//	err = db.Close()
//	assert.Nil(t, err)
//	db, err = Open(cfg)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//	err = db.Put(key, val)
//	assert.Nil(t, err)
//}

// TestDB_Get is a unit test function for the Get method of the DB struct.
//func TestDB_Get(t *testing.T) {
//	// Create a temporary directory for testing
//	cfg := DefaultConfig
//	cfg.DataFileSize = 64 * 1024 * 1024
//	temp, err := os.MkdirTemp("", "bitcask-test-get")
//	assert.Nil(t, err)
//	cfg.DirPath = temp
//
//	// Open a new DB instance
//	db, err := Open(cfg)
//
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	// Test getting an existing key
//	key := utils.GetTestKey(1)
//	val := utils.GetTestValue(24)
//	// Put the key-value pair into the database
//	err = db.Put(key, val)
//	assert.Nil(t, err)
//	// Get the value associated with the key
//	get1, err := db.Get(key)
//	assert.Nil(t, err)
//	assert.Equal(t, get1, val)
//
//	// Test getting a non-existing key
//	get2, err := db.Get([]byte("unknown key"))
//	assert.Nil(t, get2)
//	assert.Equal(t, err, ErrKeyNotFound)
//
//	// Test updating and getting a key
//	valNew := utils.GetTestValue(24)
//	err = db.Put(key, valNew)
//	assert.Nil(t, err)
//	get3, err := db.Get(key)
//	assert.Nil(t, err)
//	assert.Equal(t, get3, valNew)
//
//	// Test deleting and getting a key
//	err = db.Delete(key)
//	assert.Nil(t, err)
//	get4, err := db.Get(key)
//	assert.Equal(t, 0, len(get4))
//	assert.Equal(t, ErrKeyNotFound, err)
//
//	// Test getting a key from old files
//	for i := 0; i < 1000000; i++ {
//		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
//		assert.Nil(t, err)
//	}
//	assert.Equal(t, 2, len(db.oldFile))
//	get5, err := db.Get(utils.GetTestKey(1))
//	assert.Nil(t, err)
//	assert.NotNil(t, get5)
//
//	// Restart the database and verify the deletion
//	err = db.Close()
//	assert.Nil(t, err)
//	db, err = Open(cfg)
//	assert.Nil(t, err)
//	get6, err := db.Get(key)
//	assert.Nil(t, err)
//	assert.NotNil(t, get6)
//}

// TestDB_Delete is a unit test function for the Delete method of the DB struct.
func TestDB_Delete(t *testing.T) {
	// Create a temporary directory for testing
	cfg := DefaultConfig
	temp, err := os.MkdirTemp("", "bitcask-test-delete")
	assert.Nil(t, err)
	cfg.DirPath = temp

	// Open a new DB instance
	db, err := Open(cfg)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Delete an existing key
	key := utils.GetTestKey(1)
	val := utils.GetTestValue(24)
	err = db.Put(key, val)
	assert.Nil(t, err)
	err = db.Delete(key)
	assert.Nil(t, err)

	// Delete a non-existing key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// Delete an empty key
	err = db.Delete(nil)
	assert.Equal(t, err, ErrKeyIsEmpty)

	// Delete a key and then put it again
	err = db.Put(key, val)
	assert.Nil(t, err)
	err = db.Delete(key)
	assert.Nil(t, err)

	// Put a key and then get it to verify the deletion
	err = db.Put(key, val)
	assert.Nil(t, err)
	get, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, get, val)

	// Restart the database and verify the deletion
	err = db.Close()
	assert.Nil(t, err)
	db, err = Open(cfg)
	_, err = db.Get([]byte("unknown key"))
	assert.Equal(t, ErrKeyNotFound, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, val2)

}

// TestDB_ListKeys is a unit test function for the ListKeys method of the DB struct.
func TestDB_ListKeys(t *testing.T) {
	// Create a temporary directory for testing
	cfg := DefaultConfig
	temp, err := os.MkdirTemp("", "bitcask-test-list-keys")
	assert.Nil(t, err)
	cfg.DirPath = temp

	// Open a new DB instance
	db, err := Open(cfg)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Test when the database is empty
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// Test when there is only one key-value pair
	err = db.Put(utils.GetTestKey(10), utils.GetTestValue(10))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// Test when there are multiple key-value pairs
	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(12), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(13), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(14), utils.GetTestValue(10))
	assert.Nil(t, err)
	keys3 := db.ListKeys()
	assert.Equal(t, 5, len(keys3))
}

// TestDB_Fold is a test function for the Fold method of the DB struct.
func TestDB_Fold(t *testing.T) {
	// Create a temporary directory for testing
	cfg := DefaultConfig
	temp, err := os.MkdirTemp("", "bitcask-test-fold")
	assert.Nil(t, err)
	cfg.DirPath = temp

	// Open a new DB instance
	db, err := Open(cfg)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Put some test data into the DB
	err = db.Put(utils.GetTestKey(10), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(11), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(12), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(13), utils.GetTestValue(10))
	assert.Nil(t, err)

	// Fold over the DB and perform some assertions
	err = db.Fold(func(key, value []byte) bool {
		assert.NotNil(t, key)
		return true
	})
	assert.Nil(t, err)
}

// TestDB_Close is a unit test function for the Close method of the DB struct.
func TestDB_Close(t *testing.T) {
	// Create a temporary directory for testing
	cfg := DefaultConfig
	temp, err := os.MkdirTemp("", "bitcask-test-close")
	assert.Nil(t, err)
	cfg.DirPath = temp

	// Open a new DB instance
	db, err := Open(cfg)

	assert.Nil(t, err)
	assert.NotNil(t, db)
}

// TestDB_Sync is a unit test function for the Sync method of the DB struct.
func TestDB_Sync(t *testing.T) {
	// Create a temporary directory for testing
	cfg := DefaultConfig
	temp, err := os.MkdirTemp("", "bitcask-test-sync")
	assert.Nil(t, err)
	cfg.DirPath = temp

	// Open a new DB instance
	db, err := Open(cfg)

	assert.Nil(t, err)
	assert.NotNil(t, db)
	err = db.Sync()
	assert.Nil(t, err)
}
