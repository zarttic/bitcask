package data

import "encoding/binary"

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

// EncodeLogRecord 对于写入数据进行编码，返回字节数组以及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}

// decodeLogRecord 解码字节数组中的Header
func decodeLogRecord(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}
func getLofRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
