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

var DefaultConfig = DBConfig{
	DirPath:      os.TempDir(),
	DataFileSize: 512 * 1024 * 1024,
	SyncWrite:    false,
	IndexType:    Btree,
}
