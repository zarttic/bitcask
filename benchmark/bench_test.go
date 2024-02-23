package benchmark

import (
	"bitcask"
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

var db *bitcask.DB

func init() {
	cfg := bitcask.DefaultConfig
	dir, err := os.MkdirTemp("", "bitcask-bench-test")
	cfg.DirPath = dir
	if err != nil {
		panic(err)
	}
	db, err = bitcask.Open(cfg)
	if err != nil {
		panic(err)
	}
}
func BenchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.GetTestValue(1024))
		assert.Nil(b, err)
	}
}
