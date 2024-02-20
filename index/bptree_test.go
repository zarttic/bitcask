package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

// TestNewBPlusTree 测试函数用于测试NewBPlusTree函数
func TestNewBPlusTree(t *testing.T) {
	// 获取临时目录路径
	path := os.TempDir()
	// 创建BPlusTree对象
	tree := NewBPlusTree(path, false)

	// 在函数结束时删除临时目录
	defer func() {
		_ = os.RemoveAll(path)
	}()

	// 断言BPlusTree对象不为nil
	assert.NotNil(t, tree)
}

// TestBPlusTree_Put 测试函数用于测试BPlusTree的Put方法
func TestBPlusTree_Put(t *testing.T) {
	// 获取临时目录路径
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)
	// 创建BPlusTree对象
	tree := NewBPlusTree(path, false)
	// 在函数结束时删除临时目录
	defer func() {
		_ = os.RemoveAll(path)
	}()

	// 断言BPlusTree对象不为nil
	assert.NotNil(t, tree)

	// 调用Put方法，将"aac"和LogRecordPos对象插入BPlusTree
	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	// 断言返回结果不为nil
	assert.NotNil(t, res1)

	// 调用Put方法，将"abc"和LogRecordPos对象插入BPlusTree
	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	// 断言返回结果不为nil
	assert.NotNil(t, res2)

	// 调用Put方法，将"acc"和LogRecordPos对象插入BPlusTree
	res3 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	// 断言返回结果不为nil
	assert.NotNil(t, res3)
}

// TestBPlusTree_Get 测试BPlusTree的Get方法
func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	// 获取不存在的元素
	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	// 添加元素并获取
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)

	// 更新元素并获取
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 9884, Offset: 1232})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)
}

// TestBPlusTree_Delete 测试BPlusTree的Delete方法
func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	// 删除不存在的元素
	ok1 := tree.Delete([]byte("not exist"))
	assert.False(t, ok1)

	// 添加元素并删除
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	ok2 := tree.Delete([]byte("aac"))
	assert.True(t, ok2)

	// 获取已删除的元素
	pos1 := tree.Get([]byte("aac"))
	assert.Nil(t, pos1)
}
func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	assert.Equal(t, 0, tree.Size())

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})

	assert.Equal(t, 3, tree.Size())
}
func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("caac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbca"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acce"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("ccec"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbba"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
