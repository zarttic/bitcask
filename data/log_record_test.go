package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	//LogRecordNormal
	rec1 := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}
	res1, n := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, n, int64(5))
	//value为空
	rec2 := &LogRecord{
		Key:  []byte("key"),
		Type: LogRecordNormal,
	}
	res2, n := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n, int64(5))
	//删除测试
	rec3 := &LogRecord{
		Key:   []byte("deleteKey"),
		Value: []byte("deleteValue"),
		Type:  LogRecordDeleted,
	}
	res3, n := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, n, int64(5))
}
func TestDecodeLogRecordHeader(t *testing.T) {

	headerBuf := []byte{186, 103, 192, 80, 0, 6, 10}
	res, n := decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, res)
	assert.Greater(t, n, int64(5))
	assert.Equal(t, res, res, headerBuf)
	assert.Equal(t, res.crc, uint32(1354786746))
	assert.Equal(t, res.recordType, LogRecordNormal)
	assert.Equal(t, res.keySize, uint32(3))
	assert.Equal(t, res.valueSize, uint32(5))

	headerBuf = []byte{184, 38, 83, 75, 0, 6, 0}
	res, n = decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, res)
	assert.Greater(t, n, int64(5))
	assert.Equal(t, res, res, headerBuf)
	assert.Equal(t, res.crc, uint32(1263740600))
	assert.Equal(t, res.recordType, LogRecordNormal)
	assert.Equal(t, res.keySize, uint32(3))
	assert.Equal(t, res.valueSize, uint32(0))

	headerBuf = []byte{190, 90, 126, 234, 1, 18, 22}
	res, n = decodeLogRecordHeader(headerBuf)
	assert.NotNil(t, res)
	assert.Greater(t, n, int64(5))
	assert.Equal(t, res, res, headerBuf)
	assert.Equal(t, res.crc, uint32(3934149310))
	assert.Equal(t, res.recordType, LogRecordDeleted)
	assert.Equal(t, res.keySize, uint32(9))
	assert.Equal(t, res.valueSize, uint32(11))

}
func TestGetLogRecordCRC(t *testing.T) {
	rec := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
		Type:  LogRecordNormal,
	}
	headerBuf := []byte{186, 103, 192, 80, 0, 6, 10}
	crc := getLofRecordCRC(rec, headerBuf[crc32.Size:])
	assert.Equal(t, crc, uint32(1354786746))

	rec = &LogRecord{
		Key:  []byte("key"),
		Type: LogRecordNormal,
	}
	headerBuf = []byte{184, 38, 83, 75, 0, 6, 0}
	crc = getLofRecordCRC(rec, headerBuf[crc32.Size:])
	assert.Equal(t, crc, uint32(1263740600))

	rec = &LogRecord{
		Key:   []byte("deleteKey"),
		Value: []byte("deleteValue"),
		Type:  LogRecordNormal,
	}
	headerBuf = []byte{190, 90, 126, 234, 1, 18, 22}
	crc = getLofRecordCRC(rec, headerBuf[crc32.Size:])
	assert.Equal(t, crc, uint32(3934149310))

}
