package bitcask

import "os"

type DBConfig struct {
	// 数据库数据目录
	DirPath string
	// 数据文件大小
	DataFileSize int64
	// 每次写入数据后是否持久化
	// TODO 改成 有xx概率进行持久化？
	SyncWrite bool
	//索引类型
	IndexType IndexerType
}
type IndexerType = int8

const (
	// Btree 索引
	Btree IndexerType = iota + 1
	// ART 自适应基数树索引
	ART
)

// IteratorConfig 索引迭代器配置项
type IteratorConfig struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
}

// WriteBatchConfig represents the configuration for a write batch.
type WriteBatchConfig struct {
	// MaxBatchNum is the maximum number of data in a batch.
	MaxBatchNum uint
	// SyncWrites determines whether to persist the transaction when committing.
	SyncWrites bool
}

// DefaultConfig is the default configuration for the DB.
var DefaultConfig = DBConfig{
	DirPath:      os.TempDir(),      // Set the directory path to the temporary directory.
	DataFileSize: 512 * 1024 * 1024, // Set the data file size to 512 MB.
	SyncWrite:    false,             // Disable synchronous write.
	IndexType:    Btree,             // Use Btree index type.
}
var DefaultIteratorConfig = IteratorConfig{
	Prefix:  nil,
	Reverse: false,
}
var DefaultWriteBatchConfig = WriteBatchConfig{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
