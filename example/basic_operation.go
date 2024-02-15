package main

import (
	"bitcask"
	"fmt"
)

func main() {
	config := bitcask.DefaultConfig
	db, err := bitcask.Open(config)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("key"), []byte("value"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("key"))
	if err != nil {
		panic(err)
	}
}
