package bitcask

type DBConfig struct {
	// 数据库数据目录
	DirPath string
	// 数据文件大小
	DataFileSize int64
	// 每次写入数据后是否持久化
	// TODO 改成 有xx概率进行持久化？
	SyncWrite bool
}
