package index

import (
	"bitcask/data"
	"sync"
)
import "github.com/google/btree"

// BTree 索引，主要封装了 google 的 btree ku
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex // 写操作并发不安全，读操作并发安全
}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), //叶子节点数量
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	//空直接返回
	if btreeItem == nil {
		return nil
	}
	// 转换结构
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	// 如果原本不存在，则本次删除为一次无效的操作
	if oldItem == nil {
		return false
	}
	return true
}
