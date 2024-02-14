package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
// 实例各种资源 活跃文件，旧文件
type DB struct {
	//配置项
	cfg DBConfig
	// 互斥锁
	mu *sync.RWMutex
	//文件id只能用于加载文件索引时使用，不能在其他地方使用
	fileIDs []int
	//活跃文件 用于写入
	activateFile *data.DataFile
	// 旧数据文件，只用于读出
	oldFile map[uint32]*data.DataFile
	//内存索引
	index index.Indexer
}

// Open 打开bitcask存储引擎示例
func Open(cfg DBConfig) (*DB, error) {
	// 对传入配置进行校验

	if err := checkConfig(cfg); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在则需要创建
	if _, err := os.Stat(cfg.DirPath); os.IsNotExist(err) {
		//os.ModePerm 默认权限，允许所有用户读写
		if err := os.MkdirAll(cfg.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//初试化DB实例
	db := &DB{
		cfg:     cfg,
		mu:      new(sync.RWMutex),
		oldFile: make(map[uint32]*data.DataFile),
		index:   index.NewIndexer(cfg.IndexType),
	}
	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	//从数据文件中加载索引
	if err := db.loadIndexFromFiles(); err != nil {
		return nil, err
	}
	return db, nil

}

// Put 写入数据 key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// key 不能为空
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	//追加到当前活跃文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	//更新内存索引
	if !db.index.Put(key, pos) {
		//索引更新失败
		return ErrIndexUpdateFailed
	}
	return nil
}

// appendLogRecord 追加写入到活跃的文件中
// 返回数据的索引信息，内存索引会去存放这个数据
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	//判断当前活跃文件是否存在,在数据库没有被写入的时候是没有任何文件的
	// 如果为空则需要初试化
	if db.activateFile == nil {

		if err := db.setActivateFile(); err != nil {
			return nil, err
		}
	}
	//写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，打开新的文件
	if db.activateFile.WriteOff+size > db.cfg.DataFileSize {
		//将当前文件持久化,保证已有的数据保存到磁盘

		if err := db.activateFile.Sync(); err != nil {
			return nil, err
		}
		//持久化之后，将当前的活跃文件转化为旧文件
		db.oldFile[db.activateFile.FileID] = db.activateFile
		//打开新的数据文件

		if err := db.setActivateFile(); err != nil {
			return nil, err
		}
	}
	// 记录当前文件offset
	offset := db.activateFile.WriteOff
	//写入文件

	if err := db.activateFile.Write(encRecord); err != nil {
		return nil, err
	}
	//每次写入之后是否要对数据进行持久化，提升安全性，但是性能会下降
	if db.cfg.SyncWrite {

		if err := db.activateFile.Sync(); err != nil {
			return nil, err
		}
	}
	//构造内存索引信息并返回
	pos := &data.LogRecordPos{
		Fid:    db.activateFile.FileID,
		Offset: offset,
	}
	return pos, nil
}

// setActivateFile 设置当前活跃文件
// 需要添加互斥锁访问
func (db *DB) setActivateFile() error {
	var initialFileId uint32 = 0
	if db.activateFile != nil {
		initialFileId = db.activateFile.FileID + 1
	}
	// 打开新的活跃文件
	file, err := data.OpenDataFile(db.cfg.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activateFile = file
	return nil
}

// Get 根据key获取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// key 不能为空
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	//从内存中取出key对应的索引信息
	pod := db.index.Get(key)
	if pod == nil {
		return nil, ErrKeyNotFound
	}
	//根据文件id找到对应的文件
	var dataFile *data.DataFile
	if db.activateFile.FileID == pod.Fid {
		dataFile = db.activateFile
	} else {
		//活跃文件中没有找旧文件
		dataFile = db.oldFile[pod.Fid]
	}
	//数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//根据偏移读取数据
	record, _, err := dataFile.ReadLogRecord(pod.Offset)
	if err != nil {
		return nil, err
	}
	//判断是否是被删除的
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// checkConfig 检查配置项是
func checkConfig(cfg DBConfig) error {
	if cfg.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if cfg.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}

// loadDataFiles 从磁盘中加载文件
func (db *DB) loadDataFiles() error {
	//根据配置项读取目录
	dir, err := os.ReadDir(db.cfg.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	// 遍历目录中的所有文件,找到所有以.data结尾的文件
	for _, entry := range dir {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//分割文件名称,取出文件ID
			split := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(split[0])
			// 数据目录可能已经损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileID)

		}
	}
	//依次加载数据文件
	sort.Ints(fileIds)
	db.fileIDs = fileIds
	//遍历每个文件ID，打开对应的数据文件
	for i, fid := range fileIds {
		file, err := data.OpenDataFile(db.cfg.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		//最后的一个也就是最新的一个是活跃文件
		if i == len(fileIds)-1 {
			db.activateFile = file
		} else { //其他的也就是旧文件
			db.oldFile[uint32(fid)] = file
		}
	}
	return nil
}

// loadIndexFromFiles 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromFiles() error {
	//长度为0说明数据库为空
	if len(db.fileIDs) == 0 {
		return nil
	}
	//遍历文件id，处理文件中的记录
	for i, fid := range db.fileIDs {
		var fileID = uint32(fid)
		var dataFile *data.DataFile
		if fileID == db.activateFile.FileID {
			dataFile = db.activateFile
		} else {
			dataFile = db.oldFile[fileID]
		}
		var offset int64 = 0
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//说明文件读取完成
				if err == io.EOF {
					break
				}
				return err
			}
			//构建内存索引并保存
			pos := &data.LogRecordPos{
				Fid:    fileID,
				Offset: offset,
			}
			//被删除的索引
			if record.Type == data.LogRecordDeleted {
				db.index.Delete(record.Key)
			} else {
				db.index.Put(record.Key, pos)
			}
			offset += size
		}
		//如果当前是活跃文件，更新活跃文件的偏移
		if i == len(db.fileIDs)-1 {
			db.activateFile.WriteOff = offset
		}
	}
	return nil
}

// Delete 先写入到磁盘，之后再从内存索引中删除key
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	//从内存索引查找key
	if pod := db.index.Get(key); pod == nil {
		return nil
	}
	//构造logRecord信息，标记删除信息
	logRecord := &data.LogRecord{
		Type: data.LogRecordDeleted,
		Key:  key,
	}
	//写入到数据文件中
	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	//删除内存索引中的key
	if !db.index.Delete(key) {
		return ErrIndexUpdateFailed
	}
	return nil
}
