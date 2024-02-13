package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destoryTestFile(name string) {
	err := os.RemoveAll(name)
	if err != nil {

	}
}
func TestNewFileIOManager(t *testing.T) {
	//path := filepath.Join("../temp", "test.data")
	path := filepath.Join("/tmp", "test.data")

	fio, err := NewFileIOManager(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	defer func() {
		_ = fio.Close()
		destoryTestFile(path)
	}()

}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "test.data")

	fio, err := NewFileIOManager(path)

	defer func() {
		_ = fio.Close()
		destoryTestFile(path)
	}()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	w, err := fio.Write([]byte(""))
	assert.Equal(t, 0, w)
	assert.Nil(t, err)

	w, err = fio.Write([]byte("456"))
	assert.Equal(t, 3, w)
	assert.Nil(t, err)

	w, err = fio.Write([]byte("0123456789"))
	assert.Equal(t, 10, w)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "test.data")

	fio, err := NewFileIOManager(path)

	defer func() {
		_ = fio.Close()
		destoryTestFile(path)
	}()

	assert.Nil(t, err)
	assert.NotNil(t, fio)
	w, err := fio.Write([]byte("1234567890"))
	assert.Equal(t, 10, w)
	assert.Nil(t, err)

	b := make([]byte, 5)
	r, err := fio.Read(b, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, r)
	assert.Equal(t, []byte("12345"), b)

	b2 := make([]byte, 5)
	r2, err := fio.Read(b2, 5)
	assert.Nil(t, err)
	assert.Equal(t, 5, r2)
	assert.Equal(t, []byte("67890"), b2)
}
func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "test.data")

	fio, err := NewFileIOManager(path)

	defer func() {
		destoryTestFile(path)
	}()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}
func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "test.data")

	fio, err := NewFileIOManager(path)

	defer func() {
		destoryTestFile(path)
	}()

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
