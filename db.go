package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"bitcask/utils"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const seqNoKey = "seq.no"

// DB bitcask 存储引擎实例
// 实例各种资源 活跃文件，旧文件
type DB struct {
	cfg            DBConfig                  // 配置项
	mu             *sync.RWMutex             // 互斥锁
	fileIDs        []int                     // 文件id只能用于加载文件索引时使用，不能在其他地方使用
	activeFile     *data.DataFile            // 活跃文件 用于写入
	oldFile        map[uint32]*data.DataFile // 旧数据文件，只用于读出
	index          index.Indexer             // 内存索引
	seqNo          uint64                    // 序列号
	isMerging      bool                      //是否正在merge
	seqNoFileExist bool                      // 存储事务序列号的文件是否存在
	isInitiated    bool                      // 是否是第一次初始化数据目录
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  // key 的总数量
	DataFileNum     uint  // 数据文件的数量
	ReclaimableSize int64 // 可以进行 merge 回收的数据量，字节为单位
	DiskSize        int64 // 数据目录所占磁盘空间大小
}

// Open 打开bitcask存储引擎示例
func Open(cfg DBConfig) (*DB, error) {
	// 对传入配置进行校验

	if err := checkConfig(cfg); err != nil {
		return nil, err
	}
	var isInitiated bool
	// 判断数据目录是否存在，如果不存在则需要创建
	if _, err := os.Stat(cfg.DirPath); os.IsNotExist(err) {
		isInitiated = true
		//os.ModePerm 默认权限，允许所有用户读写
		if err := os.MkdirAll(cfg.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	dir, err := os.ReadDir(cfg.DirPath)
	if err != nil {
		return nil, err
	}
	if len(dir) == 0 {
		isInitiated = true
	}
	//初试化DB实例
	db := &DB{
		cfg:         cfg,
		mu:          new(sync.RWMutex),
		oldFile:     make(map[uint32]*data.DataFile),
		index:       index.NewIndexer(cfg.IndexType, cfg.DirPath, cfg.SyncWrite),
		isInitiated: isInitiated,
	}
	// 加载merge目录
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}
	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	// b+树索引不需要从数据文件中加载索引
	if cfg.IndexType != BPTree {
		//从hint文件中加载索引（如果有的话）
		if err := db.loadIndexFromFiles(); err != nil {
			return nil, err
		}
		//从数据文件中加载索引
		if err := db.loadIndexFromFiles(); err != nil {
			return nil, err
		}
	} else { //取出当前事务序列号
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}

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
		Key:   logRecordKeyWriteWithSeq(key, nonTransactionSeqNo), //非事务
		Value: value,
		Type:  data.LogRecordNormal,
	}
	//追加到当前活跃文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
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
	//判断当前活跃文件是否存在,在数据库没有被写入的时候是没有任何文件的
	// 如果为空则需要初试化
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	//写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，打开新的文件
	if db.activeFile.WriteOff+size > db.cfg.DataFileSize {
		//将当前文件持久化,保证已有的数据保存到磁盘

		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//持久化之后，将当前的活跃文件转化为旧文件
		db.oldFile[db.activeFile.FileID] = db.activeFile
		//打开新的数据文件

		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	// 记录当前文件offset
	offset := db.activeFile.WriteOff
	//写入文件

	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	//每次写入之后是否要对数据进行持久化，提升安全性，但是性能会下降
	if db.cfg.SyncWrite {

		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	//构造内存索引信息并返回
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: offset,
	}
	return pos, nil
}

// appendLogRecordWithLock 追加写入到活跃的文件中
// 返回数据的索引信息，内存索引会去存放这个数据
// 带锁版本
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// setActiveFile 设置当前活跃文件
// 需要添加互斥锁访问
func (db *DB) setActiveFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileID + 1
	}
	// 打开新的活跃文件
	file, err := data.OpenDataFile(db.cfg.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = file
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
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(pos)
}
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	//根据文件id找到对应的文件
	var dataFile *data.DataFile
	if db.activeFile.FileID == pos.Fid {
		dataFile = db.activeFile
	} else {
		//活跃文件中没有找旧文件
		dataFile = db.oldFile[pos.Fid]
	}
	//数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//根据偏移读取数据
	record, _, err := dataFile.ReadLogRecord(pos.Offset)
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
			db.activeFile = file
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
	// 从hint文件中已经加载过了
	hasMerge, nonMergeFileID := false, uint32(0)
	fileName := filepath.Join(db.cfg.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(fileName); err == nil {
		fileID, err := db.getNonMergeFileID(db.cfg.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileID = fileID
	}

	updateIndex := func(key []byte, ty data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if ty == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}
	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	//遍历文件id，处理文件中的记录
	for i, fid := range db.fileIDs {
		var fileID = uint32(fid)
		// 从hint文件加载过了，不需要重新加载
		if hasMerge && fileID == nonMergeFileID {
			continue
		}
		var dataFile *data.DataFile
		if fileID == db.activeFile.FileID {
			dataFile = db.activeFile
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
			// 解析key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(record.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务提交，直接更新内存索引
				updateIndex(realKey, record.Type, pos)
			} else {
				//事务提交
				// 事务完成后，更新到内存
				if record.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					record.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: record,
						Pos:    pos,
					})
				}
			}
			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			//递增offset
			offset += size
		}
		//如果当前是活跃文件，更新活跃文件的偏移
		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOff = offset
		}
	}
	//更新事务序列号
	db.seqNo = currentSeqNo
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
		Key:  logRecordKeyWriteWithSeq(key, nonTransactionSeqNo),
	}
	//写入到数据文件中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	//删除内存索引中的key
	if !db.index.Delete(key) {
		return ErrIndexUpdateFailed
	}
	return nil
}

// ListKeys returns a list of keys in the database.
func (db *DB) ListKeys() [][]byte {
	// Get an iterator for the index.
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	// Create a slice to store the keys.
	keys := make([][]byte, db.index.Size())

	// Initialize the index of the keys slice.
	var idx int

	// Iterate over the index using the iterator.
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		// Get the key from the iterator and store it in the keys slice.
		keys[idx] = iterator.Key()
		idx++
	}

	// Return the keys slice.
	return keys
}

// Fold 获取所有的数据并执行指定的操作
// Fold applies the given function to each key-value pair in the database.
// The function returns true if the iteration should continue, or false to stop.
// The function is called with the key and value of each pair.
// The function must be safe for concurrent access.
// The Fold function returns an error if there is a problem iterating over the database.
func (db *DB) Fold(fn func(key, value []byte) bool) error {
	// Acquire a read lock on the database.
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Get an iterator for the index.
	iterator := db.index.Iterator(false)

	// Iterate over the index using the iterator.
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		// Get the key from the iterator.
		key := iterator.Key()

		// Get the value for the key from the database.
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		// Apply the function to the key-value pair.
		if !fn(key, value) {
			break
		}
	}

	// Return nil if the iteration was successful.
	return nil
}

// Close closes the DB connection and releases any resources.
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.index.Close(); err != nil {
		return err
	}
	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.cfg.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// Close the current active file.
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// Close the old data files.
	for _, file := range db.oldFile {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// loadSeqNo 从指定路径加载序列号文件并获取最新的序列号
func (db *DB) loadSeqNo() error {
	// 拼接序列号文件路径
	path := filepath.Join(db.cfg.DirPath, data.SeqNoFileName)
	// 判断文件是否存在
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	// 打开序列号文件
	file, err := data.OpenSeqNoFile(db.cfg.DirPath)
	if err != nil {
		return err
	}
	// 读取文件第一条日志记录
	record, _, err := file.ReadLogRecord(0)
	if err != nil {
		return err
	}
	// 将日志记录的值转换为序列号
	seq, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	// 更新数据库的序列号
	db.seqNo = seq
	db.seqNoFileExist = true
	return nil
}
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.oldFile))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.cfg.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:      uint(db.index.Size()),
		DataFileNum: dataFiles,
		DiskSize:    dirSize,
	}
}
