package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    2,
		Offset: 101,
	})
	assert.True(t, res2)
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(2), pos2.Fid)
	assert.Equal(t, int64(101), pos2.Offset)

	res3 := bt.Put([]byte("b"), &data.LogRecordPos{
		Fid:    3,
		Offset: 102,
	})
	assert.True(t, res3)
	pos3 := bt.Get([]byte("b"))
	assert.Equal(t, uint32(3), pos3.Fid)
	assert.Equal(t, int64(102), pos3.Offset)

}
func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)
	assert.True(t, bt.Delete(nil))
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res2)
	assert.True(t, bt.Delete([]byte("a")))
}
