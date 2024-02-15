package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)
const (
	//crc 4 type 1 keySize 5 valueSize 5 = 15
	maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5
)

// LogRecordPos 内存索引信息，主要是描述数据在磁盘上的位置
// 内存中
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示将数据存储到了哪个文件当中
	Offset int64  // 偏移，表示将数据存储到了数据文件中的哪个位置
}

// LogRecord 写入到数据文件的记录
// 因为是类似日志追加写入文件，所以叫日志
// 盘上的信息
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// logRecordHeader 日志头 长度15字节
type logRecordHeader struct {
	//crc 校验值 4 [单位:字节]
	crc uint32
	//记录标识 1
	recordType LogRecordType
	//key长度 5
	keySize uint32
	//value长度 5
	valueSize uint32
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	//初始化 header 部分的字节数组
	header := make([]byte, maxLogRecordHeaderSize)
	//第5个字节存储type
	header[4] = record.Type
	var index = 5

	// 之后存放的是kvSize
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))
	//index 为header的总长度
	var totalSize = index + len(record.Key) + len(record.Value)
	//编码后的字节数组
	encBytes := make([]byte, totalSize)
	//将header部分拷贝过来
	copy(encBytes[:index], header[:index])
	//将kv数据直接拷贝过来
	copy(encBytes[index:], record.Key)
	copy(encBytes[index+len(record.Key):], record.Value)
	//对于数据进行校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	//写入到头部，（LittleEndian 以小端去写）
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(totalSize)
}

// decodeLogRecord 解码字节数组中的Header
func decodeLogRecord(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	//读出kv size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(buf[index:])
	index += n
	header.valueSize = uint32(valueSize)

	return header, int64(index)
}
func getLofRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	//引用
	//crc := crc32.ChecksumIEEE(header)
	//拷贝
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
