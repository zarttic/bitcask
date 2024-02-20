package index

import (
	"bitcask/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree 初始化 B+ 树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的 bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

// Put 将给定的键值对存储到BPlusTree中
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value to bptree")
	}
	return true
}

// Get 从BPlusTree中获取给定键对应的值
func (bpt *BPlusTree) Get(key []byte) (pos *data.LogRecordPos) {

	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value from bptree")
	}
	return pos
}

// Delete 从BPlusTree中删除给定键对应的值
func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	return ok
}

// Size 返回BPlusTree中存储的键值对数量
func (bpt *BPlusTree) Size() (size int) {
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size from bptree")
	}
	return size
}
func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {

	return newBptreeIterator(bpt.tree, reverse)
}

// bptreeIterator 是用于遍历BPlusTree的迭代器结构体
type bptreeIterator struct {
	tx        *bbolt.Tx     // 存储当前事务对象
	cursor    *bbolt.Cursor // 存储当前游标对象
	reverse   bool          // 是否反向遍历
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) (bpi *bptreeIterator) {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin transaction")
	}
	bpi = &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return
}
func (b *bptreeIterator) Rewind() {
	if b.reverse {
		b.currKey, b.currValue = b.cursor.Last()
	} else {
		b.currKey, b.currValue = b.cursor.First()
	}
}

func (b *bptreeIterator) Seek(key []byte) {
	b.currKey, b.currValue = b.cursor.Seek(key)
}

func (b *bptreeIterator) Next() {
	if b.reverse {
		b.currKey, b.currValue = b.cursor.Prev()
	} else {
		b.currKey, b.currValue = b.cursor.Next()

	}
}

func (b *bptreeIterator) Valid() bool {
	return len(b.currKey) != 0
}

func (b *bptreeIterator) Key() []byte {
	return b.currKey
}

func (b *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(b.currValue)

}

func (b *bptreeIterator) Close() {
	_ = b.tx.Rollback()
}
