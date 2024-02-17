package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// TestDB_NewWriteBatch is a unit test for the NewWriteBatch function.
func TestDB_NewWriteBatch(t *testing.T) {
	// Create a default configuration for the Bitcask database.
	cfg := DefaultConfig
	// Create a temporary directory for the database.
	dir, err := os.MkdirTemp("", "bitcask-test-batch-new")
	assert.Nil(t, err)
	cfg.DirPath = dir
	// Open the Bitcask database.
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Create a new write batch.
	batch := db.NewWriteBatch(DefaultWriteBatchConfig)
	assert.NotNil(t, batch)
}

// TestWriteBatch_1 is a unit test for the WriteBatch function.
func TestWriteBatch_1(t *testing.T) {
	// Create a default configuration for the Bitcask database.
	cfg := DefaultConfig
	// Create a temporary directory for the database.
	dir, err := os.MkdirTemp("", "bitcask-test-batch-write1")
	assert.Nil(t, err)
	cfg.DirPath = dir
	// Open the Bitcask database.
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Create a new write batch without committing any data.
	batch1 := db.NewWriteBatch(DefaultWriteBatchConfig)
	assert.NotNil(t, batch1)
	// Put a key-value pair into the write batch.
	err = batch1.Put(utils.GetTestKey(10), utils.GetTestValue(10))
	assert.Nil(t, err)
	// Delete a key from the write batch.
	err = batch1.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	// Retrieve the value of a key that was not committed.
	get1, err := db.Get(utils.GetTestKey(10))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Equal(t, len(get1), 0)

	// Commit the write batch, making the changes permanent.
	err = batch1.Commit()
	assert.Nil(t, err)
	// Retrieve the value of the key after committing the changes.
	get2, err := db.Get(utils.GetTestKey(10))
	assert.NotNil(t, get2)
	assert.Nil(t, err)

	// Create a new write batch and delete the key.
	batch2 := db.NewWriteBatch(DefaultWriteBatchConfig)
	err = batch2.Delete(utils.GetTestKey(10))
	assert.Nil(t, err)
	// Commit the write batch, making the deletion permanent.
	err = batch2.Commit()
	assert.Nil(t, err)
	// Retrieve the value of the key after committing the deletion.
	get3, err := db.Get(utils.GetTestKey(10))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Equal(t, len(get3), 0)
}

// TestWriteBatch_2 is a unit test for the WriteBatch function.
func TestWriteBatch_2(t *testing.T) {
	cfg := DefaultConfig
	dir, err := os.MkdirTemp("", "bitcask-test-batch-write2")
	assert.Nil(t, err)
	cfg.DirPath = dir
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Create a new WriteBatch
	batch1 := db.NewWriteBatch(DefaultWriteBatchConfig)
	assert.NotNil(t, batch1)

	// Put an existing data
	err = batch1.Put(utils.GetTestKey(11), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = batch1.Put(utils.GetTestKey(12), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = batch1.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)

	// Commit the batch
	err = batch1.Commit()
	assert.Nil(t, err)

	// Put another data
	err = batch1.Put(utils.GetTestKey(12), utils.GetTestValue(10))
	assert.Nil(t, err)

	// Commit the batch again
	err = batch1.Commit()
	assert.Nil(t, err)

	// Restart the database
	err = db.Close()
	assert.Nil(t, err)
	db, err = Open(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// Check if the data is retrieved correctly
	get1, err := db.Get(utils.GetTestKey(11))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Equal(t, len(get1), 0)
	assert.Equal(t, uint64(2), db.seqNo)
}

//func TestWriteBatch_3(t *testing.T) {
//	cfg := DefaultConfig
//	dir := "/temp"
//	cfg.DirPath = dir
//	t.Log(cfg.DirPath)
//	db, err := Open(cfg)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//	batch := db.NewWriteBatch(WriteBatchConfig{
//		MaxBatchNum: 5000000,
//		SyncWrites:  true,
//	})
//	//for i := 0; i < 500000; i++ {
//	//	err := batch.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
//	//	assert.Nil(t, err)
//	//}
//	err = batch.Commit()
//	assert.Nil(t, err)
//
//}
