package index

import (
	"bitcask/data"
	"bytes"
	"sort"
	"sync"
)
import "github.com/google/btree"

// BTree 索引，主要封装了 google 的 btree ku
// https://github.com/google/btree
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex // 写操作并发不安全，读操作并发安全
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)

}

// NewBTree 初始化 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), //叶子节点数量
		lock: new(sync.RWMutex),
	}
}

// Put 将 key-value 对添加到 BTree 中
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

// Get 根据 key 从 BTree 中获取对应的 value
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

// Delete 从 BTree 中删除指定的 key
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

// Size 获取数据量
func (bt *BTree) Size() int {
	return bt.tree.Len()
}
func (bt *BTree) Close() error {
	return nil
}

// BTree 迭代器
type btreeIterator struct {
	// 当前遍历的下标位置
	currIndex int
	// 是否反向遍历
	reverse bool
	// key+位置索引信息
	values []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)

	}
	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind rewinds the iterator to the beginning of the BTree.
func (bt *btreeIterator) Rewind() {
	bt.currIndex = 0
}

func (bt *btreeIterator) Seek(key []byte) {
	//btree 有序，使用二分查找加速
	if bt.reverse {
		bt.currIndex = sort.Search(len(bt.values), func(i int) bool {
			return bytes.Compare(bt.values[i].key, key) <= 0
		})
	} else {
		bt.currIndex = sort.Search(len(bt.values), func(i int) bool {
			return bytes.Compare(bt.values[i].key, key) >= 0
		})
	}
}

// Next moves the iterator to the next element in the BTree.
func (bt *btreeIterator) Next() {
	bt.currIndex++
}

// Valid returns true if the iterator is still valid, false otherwise.
func (bt *btreeIterator) Valid() bool {
	return bt.currIndex < len(bt.values)
}

// Key returns the key of the current element in the BTree.
func (bt *btreeIterator) Key() []byte {
	return bt.values[bt.currIndex].key
}

// Value returns the value of the current element in the BTree.
func (bt *btreeIterator) Value() *data.LogRecordPos {
	return bt.values[bt.currIndex].pos
}

// Close closes the iterator and releases any resources it was using.
func (bt *btreeIterator) Close() {
	bt.values = nil
}
