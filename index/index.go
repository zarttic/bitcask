package index

import (
	"bitcask/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer TODO 添加art实现
// Indexer 抽象索引接口，后续如果想要接入其他的数据结构，则直接实现这个接口即可
type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) bool
}

// IndexType 索引类型
type IndexType = int8

// Btree 索引
const (
	Btree IndexType = iota + 1
	// ART 自适应基数树索引
	ART
)

// NewIndexer 初试化索引
func NewIndexer(tp IndexType) Indexer {
	switch tp {
	case Btree:
		return NewBTree()
	case ART:
		//todo
		return nil
	default:
		panic("unsupported index type")
	}
}

// Item 索引项
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 比较函数
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
