package bitcask

import (
	"bitcask/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

// 非事务操作
const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch 原子写
type WriteBatch struct {
	cfg           WriteBatchConfig
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存写入的信息
}

// NewWriteBatch 初始化WriteBatch方法
// NewWriteBatch creates a new WriteBatch object with the given WriteBatchConfig.
func (db *DB) NewWriteBatch(cfg WriteBatchConfig) *WriteBatch {
	return &WriteBatch{
		cfg:           cfg,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put adds a key-value pair to the write batch.
// It returns an error if the key is empty.
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// Create a log record with the given key and value.
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}

	// Add the log record to the pending writes map with a lock.
	wb.mu.Lock()
	wb.pendingWrites[string(key)] = logRecord
	wb.mu.Unlock()

	return nil
}

// Delete 从 WriteBatch 中删除指定的键。
// 如果键为空，则返回 ErrKeyIsEmpty 错误。
// 键对应的值存在时，会创建一个 LogRecord 并将其添加到 pendingWrites 映射中。
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 获取键对应的值
	get := wb.db.index.Get(key)
	if get == nil {
		// 数据不存在，直接返回
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 创建LogRecord并添加到pendingWrites映射中
	wb.pendingWrites[string(key)] = &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}

	return nil
}

// Commit 提交事务，将暂存的数据写入到数据文件，更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.cfg.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	//更新事务的序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	// 开始写数据到数据文件中
	position := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWriteWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		position[string(record.Key)] = pos
	}
	// 事务完成标识
	finished := &data.LogRecord{
		Key:  logRecordKeyWriteWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	_, err := wb.db.appendLogRecord(finished)
	if err != nil {
		return err
	}
	// 进行持久化
	if wb.cfg.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	//更新内存索引
	for _, record := range wb.pendingWrites {

		pos := position[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}
	//将暂存数据清空
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// logRecordKeyWriteWithSeq key + seq num 编码
func logRecordKeyWriteWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, len(key)+n)
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
