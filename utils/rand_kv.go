package utils

import (
	"fmt"
	"math/rand"
	"time"
)

// randStr is a new instance of Rand with a new Source initialized with the current Unix time.
var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey returns a test key with a format of "key-000000000".
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("key-%09d", i))
}

// GetTestValue returns a test value with a length of n, generated by randomly selecting letters from the letters slice.
func GetTestValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("value-" + string(b))
}
