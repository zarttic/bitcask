package data

import (
	"bitcask/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

// DataFile 数据文件
type DataFile struct {
	FileID    uint32        //文件id
	WriteOff  int64         //文件偏移
	IoManager fio.IOManager //进行数据读写操作
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileID uint32) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileID)
	return newDataFile(fileName, fileID)

}

// OpenHintFile 打开hint索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// newDataFile函数用于创建一个新的DataFile对象。
// 参数fileName为文件名，fileID为文件ID。
// 返回一个指向DataFile对象的指针和一个错误对象。
func newDataFile(fileName string, fileID uint32) (*DataFile, error) {
	// 初始化IOManager管理接口
	manager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	// 返回数据文件
	return &DataFile{
		FileID:    fileID,
		WriteOff:  0,
		IoManager: manager,
	}, nil
}

// WriteHintRecord 写入索引信息到hint文件
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)

	return df.Write(encRecord)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

// ReadLogRecord 根据offset读取数据信息
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// 下一次读取超过文件最大长度，则读取到文件末尾
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, nil
	}
	header, headerSize := decodeLogRecordHeader(headerBuf)
	//读取到了文件末尾，返回EOF
	if (header == nil) || (header.crc == 0 && header.keySize == 0 && header.valueSize == 0) {
		return nil, 0, io.EOF
	}
	//取出key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	//总长度
	recordSize := headerSize + keySize + valueSize
	logRecord := &LogRecord{
		Type: header.recordType,
	}
	//读取实际存储的k/v数据
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]

	}
	//校验数据
	crc := getLofRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return

}

// Write写入文件，同时维护writeOff字段
func (df *DataFile) Write(buf []byte) error {
	write, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(write)
	return nil
}

// Sync 持久化文件
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Close 关闭文件
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}
func GetDataFileName(dirPath string, fileID uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+DataFileNameSuffix)
}

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0)
}
