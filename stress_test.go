package bitcask

import (
	"bitcask/utils"
	"fmt"
	"os"
	"sync"
	"testing"
)

// TestStress_ConcurrentPutGet 并发读写压力测试
func TestStress_ConcurrentPutGet(t *testing.T) {
	cfg := DefaultConfig
	cfg.DataFileSize = 64 * 1024 * 1024
	dir, err := os.MkdirTemp("", "bitcask-stress-concurrent")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DirPath = dir
	defer os.RemoveAll(dir)

	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numWriters = 10
	const numReaders = 10
	const opsPerGoroutine = 10000

	// 先写入一批数据用于读取
	for i := 0; i < opsPerGoroutine; i++ {
		if err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup

	// 并发写入
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			base := writerID * opsPerGoroutine
			for i := 0; i < opsPerGoroutine; i++ {
				key := utils.GetTestKey(base + i)
				val := utils.GetTestValue(128)
				if err := db.Put(key, val); err != nil {
					t.Errorf("writer %d: put error: %v", writerID, err)
					return
				}
			}
		}(w)
	}

	// 并发读取
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := utils.GetTestKey(i % opsPerGoroutine)
				_, _ = db.Get(key)
			}
		}(r)
	}

	wg.Wait()
	t.Logf("Concurrent stress test passed: %d writers x %d ops + %d readers x %d ops",
		numWriters, opsPerGoroutine, numReaders, opsPerGoroutine)
}

// TestStress_WriteBatch 并发批量写入压力测试
func TestStress_WriteBatch(t *testing.T) {
	cfg := DefaultConfig
	cfg.DataFileSize = 64 * 1024 * 1024
	dir, err := os.MkdirTemp("", "bitcask-stress-batch")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DirPath = dir
	defer os.RemoveAll(dir)

	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const numBatches = 20
	const batchSize = 500

	var wg sync.WaitGroup

	for b := 0; b < numBatches; b++ {
		wg.Add(1)
		go func(batchID int) {
			defer wg.Done()
			wb := db.NewWriteBatch(WriteBatchConfig{
				MaxBatchNum: uint(batchSize + 100),
				SyncWrites:  false,
			})
			base := batchID * batchSize
			for i := 0; i < batchSize; i++ {
				key := utils.GetTestKey(base + i)
				val := utils.GetTestValue(64)
				if err := wb.Put(key, val); err != nil {
					t.Errorf("batch %d: put error: %v", batchID, err)
					return
				}
			}
			if err := wb.Commit(); err != nil {
				t.Errorf("batch %d: commit error: %v", batchID, err)
			}
		}(b)
	}

	wg.Wait()

	// 验证数据
	var count int
	err = db.Fold(func(key, value []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("WriteBatch stress test passed: %d batches x %d ops, total keys: %d", numBatches, batchSize, count)
}

// TestStress_LargeKeyValue 大 Key/Value 压力测试
func TestStress_LargeKeyValue(t *testing.T) {
	cfg := DefaultConfig
	cfg.DataFileSize = 64 * 1024 * 1024
	dir, err := os.MkdirTemp("", "bitcask-stress-largekv")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DirPath = dir
	defer os.RemoveAll(dir)

	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 测试不同大小的 value
	sizes := []int{0, 1, 100, 1024, 64 * 1024, 256 * 1024}
	for _, size := range sizes {
		key := []byte(fmt.Sprintf("large-key-%d", size))
		val := utils.GetTestValue(size)
		if err := db.Put(key, val); err != nil {
			t.Fatalf("put size %d failed: %v", size, err)
		}
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("get size %d failed: %v", size, err)
		}
		if len(got) != len(val) {
			t.Fatalf("size mismatch: expected %d, got %d", len(val), len(got))
		}
	}
	t.Logf("Large KV stress test passed: tested sizes %v", sizes)
}

// TestStress_Restart 模拟多次重启的数据持久性测试
func TestStress_Restart(t *testing.T) {
	cfg := DefaultConfig
	cfg.DataFileSize = 64 * 1024 * 1024
	dir, err := os.MkdirTemp("", "bitcask-stress-restart")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DirPath = dir
	defer os.RemoveAll(dir)

	const numKeys = 5000
	const numRestarts = 5

	// 写入数据
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < numKeys; i++ {
		if err := db.Put(utils.GetTestKey(i), utils.GetTestValue(128)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// 多次重启验证
	for r := 0; r < numRestarts; r++ {
		db, err = Open(cfg)
		if err != nil {
			t.Fatalf("restart %d: open failed: %v", r, err)
		}
		// 验证所有 key 都存在
		for i := 0; i < numKeys; i++ {
			key := utils.GetTestKey(i)
			val, err := db.Get(key)
			if err != nil {
				t.Fatalf("restart %d: get key %d failed: %v", r, i, err)
			}
			if len(val) == 0 {
				t.Fatalf("restart %d: key %d has empty value", r, i)
			}
		}
		// 追加写入
		offset := numKeys + r*100
		for i := 0; i < 100; i++ {
			if err := db.Put(utils.GetTestKey(offset+i), utils.GetTestValue(64)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("Restart stress test passed: %d keys, %d restarts", numKeys, numRestarts)
}
