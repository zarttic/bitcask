package bitcask

import (
	"bitcask/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据 生成Hint文件
func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 一次只能有一个merge进程
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()
	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	// 讲过当前活跃文件转化为旧的数据文件
	db.oldFile[db.activeFile.FileID] = db.activeFile
	// 打开新的活跃文件
	if err := db.setActiveFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 记录没有参与merge的文件
	nonMergeFileId := db.activeFile.FileID

	// 取出需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.oldFile {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()
	// 从小到大进行merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})
	mergePath := db.getMergePath()
	//若存在merge目录，需要移除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
		return err
	}
	//新建一个merge目录
	if err := os.Mkdir(mergePath, os.ModePerm); err != nil {
		return err
	}
	//打开用于merge 的 bitcask实例
	mergeConfig := db.cfg
	mergeConfig.DirPath = mergePath
	mergeConfig.SyncWrite = false
	mergeDB, err := Open(mergeConfig)
	if err != nil {
		return err
	}
	// 打开 hint 文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	//取出记录 重写有效数据
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			record, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 获取实际的key
			realKey, _ := parseLogRecordKey(record.Key)
			pos := db.index.Get(realKey)
			//和内存中的索引位置进行比较
			if pos != nil &&
				pos.Fid == dataFile.FileID &&
				pos.Offset == offset {
				// 重写有效数据,清除事务标记
				record.Key = logRecordKeyWriteWithSeq(realKey, nonTransactionSeqNo)
				logRecordPos, err := mergeDB.appendLogRecord(record)
				if err != nil {
					return err
				}
				//将当前位置索引写入到hint文件中
				if err := hintFile.WriteHintRecord(realKey, logRecordPos); err != nil {
					return err
				}

			}
			offset += size
		}
	}
	//持久化文件
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//添加merge完成标识
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	// 比当前文件编号小的都参与了merge操作
	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
		Type:  data.LogRecordNormal,
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// getMergePath 获取merge目录
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.cfg.DirPath))
	base := path.Base(db.cfg.DirPath)
	return path.Join(dir, base+mergeDirName)
}

// 加载merge 数据目录
func (db *DB) loadMergeFile() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err != nil {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dir, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// 查找merge完成的标识
	var mergeFinished bool
	var mergeFileNames []string
	for _, file := range dir {
		if file.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, file.Name())
	}
	// 如果没有merge完成 直接返回
	if !mergeFinished {
		return nil
	}
	// 如果有标识
	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}
	// 删除对应的数据文件
	// 删除id 相对于较小的id
	var fileID uint32 = 0
	for ; fileID < nonMergeFileID; fileID++ {
		fileName := data.GetDataFileName(db.cfg.DirPath, fileID)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	// 将新的数据文件移动到数据目录中
	for _, fileName := range mergeFileNames {
		src := filepath.Join(mergePath, fileName)
		dst := filepath.Join(db.cfg.DirPath, fileName)
		if err := os.Rename(src, dst); err != nil {
			return err
		}
	}
	return nil
}
func (db *DB) getNonMergeFileID(dirPath string) (uint32, error) {
	file, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := file.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	fileID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(fileID), nil
}

func (db *DB) loadIndexFromHintFile() error {
	// 查询hint文件是否存在
	hintPath := filepath.Join(db.cfg.DirPath, data.HintFileName)
	if len(hintPath) == 0 {
		return nil
	}
	// 存在则打开
	hintFile, err := data.OpenHintFile(db.cfg.DirPath)
	if err != nil {
		return err
	}
	//直接存放到索引
	var offset int64 = 0
	for {
		record, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 拿到实际的索引
		pos := data.DecodeLogRecordPos(record.Value)
		db.index.Put(record.Key, pos)
		offset += size
	}
	return err

}
