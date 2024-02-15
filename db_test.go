package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// TODO
// put get delete
// 测试完成之后销毁目录
func destoryDB(db *DB) {
	if db.activeFile != nil {
		_ = db.activeFile.Close()
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

func TestDB_Put(t *testing.T) {
	//put一条数据
	cfg := DefaultConfig
	cfg.DataFileSize = 64 * 1024 * 1024
	temp, err := os.MkdirTemp("", "bitcask-test-put")
	assert.Nil(t, err)
	cfg.DirPath = temp
	db, err := Open(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	key := utils.GetTestKey(1)
	val := utils.GetTestValue(24)
	err = db.Put(key, val)
	assert.Nil(t, err)
	val1, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, val1)
	//put key存在的数据
	val = utils.GetTestValue(24)
	err = db.Put(key, val)
	assert.Nil(t, err)
	val2, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, val2)

	// key 为空
	err = db.Put(nil, val)
	assert.Equal(t, err, ErrKeyIsEmpty)
	// value 为空
	err = db.Put(key, nil)
	assert.Nil(t, err)
	val3, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, len(val3), 0)
	// 写入到数据文件进行转换
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.oldFile))
}
func TestDB_Get(t *testing.T) {
	//读取数据

}
func TestDB_Delete(t *testing.T) {
	cfg := DefaultConfig
	temp, err := os.MkdirTemp("", "bitcask-test-delete")
	assert.Nil(t, err)
	cfg.DirPath = temp
	db, err := Open(cfg)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	// 删除存在的key
	key := utils.GetTestKey(1)
	val := utils.GetTestValue(24)
	err = db.Put(key, val)
	assert.Nil(t, err)
	err = db.Delete(key)
	assert.Nil(t, err)
	// 删除不存在的key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)
	// 删除空的key
	err = db.Delete(nil)
	assert.Equal(t, err, ErrKeyIsEmpty)
	// 删除后重新put
	err = db.Put(key, val)
	assert.Nil(t, err)
	err = db.Delete(key)
	assert.Nil(t, err)

	err = db.Put(key, val)
	assert.Nil(t, err)
	get, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, get, val)
}
