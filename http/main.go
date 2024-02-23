package main

import (
	"bitcask"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

func init() {
	config := bitcask.DefaultConfig
	dir, err := os.MkdirTemp("", "bitcask-http")
	if err != nil {
		panic(err)
	}
	config.DirPath = dir
	db, err = bitcask.Open(config)
	if err != nil {
		panic(fmt.Errorf("failed to open bitcask: %w", err))
	}

}

// put
func handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var kv = make(map[string]string)
	if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for key, value := range kv {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get kv in db, error:%v", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(string(value))

}
func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if err := db.Delete([]byte(key)); err != nil && !errors.Is(err, bitcask.ErrKeyIsEmpty) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode("OK")
}
func handleListKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	keys := db.ListKeys()
	w.Header().Set("Content-Type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(w).Encode(result)

}
func handleStat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	stat := db.Stat()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stat)
}
func main() {
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkey", handleListKey)
	http.HandleFunc("/bitcask/stat", handleStat)

	_ = http.ListenAndServe(":8082", nil)
}
