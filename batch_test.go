package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewWriteBatch(t *testing.T) {
	cfg := DefaultConfig
	dir, err := os.MkdirTemp("", "bitcask-test-batch-new")
	assert.Nil(t, err)
	cfg.DirPath = dir
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	batch := db.NewWriteBatch(DefaultWriteBatchConfig)
	assert.NotNil(t, batch)
}
func TestWriteBatch_1(t *testing.T) {
	cfg := DefaultConfig
	dir, err := os.MkdirTemp("", "bitcask-test-batch-write1")
	assert.Nil(t, err)
	cfg.DirPath = dir
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 写不提交数据
	batch1 := db.NewWriteBatch(DefaultWriteBatchConfig)
	assert.NotNil(t, batch1)
	err = batch1.Put(utils.GetTestKey(10), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = batch1.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	get1, err := db.Get(utils.GetTestKey(10))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Equal(t, len(get1), 0)
	// 提交
	err = batch1.Commit()
	assert.Nil(t, err)
	get2, err := db.Get(utils.GetTestKey(10))
	assert.NotNil(t, get2)
	assert.Nil(t, err)
	// 提交删除
	batch2 := db.NewWriteBatch(DefaultWriteBatchConfig)
	err = batch2.Delete(utils.GetTestKey(10))
	assert.Nil(t, err)
	err = batch2.Commit()
	assert.Nil(t, err)
	get3, err := db.Get(utils.GetTestKey(10))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Equal(t, len(get3), 0)

}
func TestWriteBatch_2(t *testing.T) {
	cfg := DefaultConfig
	dir, err := os.MkdirTemp("", "bitcask-test-batch-write2")
	assert.Nil(t, err)
	cfg.DirPath = dir
	db, err := Open(cfg)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	batch1 := db.NewWriteBatch(DefaultWriteBatchConfig)
	assert.NotNil(t, batch1)
	// put一个存在的数据
	err = batch1.Put(utils.GetTestKey(11), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = batch1.Put(utils.GetTestKey(12), utils.GetTestValue(10))
	assert.Nil(t, err)
	err = batch1.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	err = batch1.Commit()
	assert.Nil(t, err)
	//重启
	err = db.Close()
	assert.Nil(t, err)
	db, err = Open(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	get1, err := db.Get(utils.GetTestKey(11))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Equal(t, len(get1), 0)

}
