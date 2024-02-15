package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// TestOpenDataFile is a test function for the OpenDataFile function.
func TestOpenDataFile(t *testing.T) {
	file1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file1)
	file2, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, file2)
	file3, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, file3)
}

// TestDataFile_Write is a test function for the Write method of the DataFile struct.
func TestDataFile_Write(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	err = file.Write([]byte("123"))
	assert.Nil(t, err)
	err = file.Write([]byte("456"))
	assert.Nil(t, err)
	err = file.Write([]byte("789"))
	assert.Nil(t, err)
}

// TestDataFile_Close is a test function for the Close method of the DataFile struct.
func TestDataFile_Close(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	err = file.Write([]byte("123"))
	assert.Nil(t, err)
	err = file.Close()
	assert.Nil(t, err)
}

// TestDataFile_Sync is a test function for the Sync method of the DataFile struct.
func TestDataFile_Sync(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	err = file.Write([]byte("123"))
	assert.Nil(t, err)
	err = file.Sync()
	assert.Nil(t, err)
}

// TestDataFile_ReadLogRecord is a test function for reading log records from a data file.
func TestDataFile_ReadLogRecord(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 103)
	assert.Nil(t, err)
	assert.NotNil(t, file)

	// Single log record write
	rec1 := &LogRecord{
		Key:   []byte("key"),
		Value: []byte("value"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = file.Write(res1)
	assert.Nil(t, err)

	// Single log record read
	readRec1, readSize1, err := file.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	// Multiple log records write
	rec2 := &LogRecord{
		Key:   []byte("key2"),
		Value: []byte("value2"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = file.Write(res2)
	assert.Nil(t, err)

	// Multiple log records read
	readRec2, readSize2, err := file.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在文件末尾
	rec3 := &LogRecord{
		Key:   []byte("key3"),
		Value: []byte("value3"),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = file.Write(res3)
	assert.Nil(t, err)

	// 读取被删除的数据
	readRec3, readSize3, err := file.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
}
