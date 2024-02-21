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
	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
	//Size 索引中的数据量
	Size() int
	Close() error
}

// IndexType 索引类型
type IndexType = int8

// Btree 索引
const (
	Btree IndexType = iota + 1
	// ART 自适应基数树索引
	ART
	BPTree //B+树
)

// NewIndexer 初试化索引
func NewIndexer(tp IndexType, dir string, sync bool) Indexer {
	switch tp {
	case Btree:
		return NewBTree()
	case ART:
		//todo
		return NewART()
	case BPTree:
		return NewBPlusTree(dir, sync)
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

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
