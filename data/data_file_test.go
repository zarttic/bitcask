package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	file1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file1)
	file2, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, file2)
	file3, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, file3)
}
func TestDataFile_Write(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	err = file.Write([]byte("123"))
	assert.Nil(t, err)
	err = file.Write([]byte("456"))
	assert.Nil(t, err)
	err = file.Write([]byte("789"))
	assert.Nil(t, err)
}
func TestDataFile_Close(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	err = file.Write([]byte("123"))
	assert.Nil(t, err)
	err = file.Close()
	assert.Nil(t, err)

}
func TestDataFile_Sync(t *testing.T) {
	file, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file)
	err = file.Write([]byte("123"))
	assert.Nil(t, err)
	err = file.Sync()
	assert.Nil(t, err)
}
