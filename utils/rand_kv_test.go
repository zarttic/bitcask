package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTestKey(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetTestKey(i)))
	}
}
func TestGetTestValue(t *testing.T) {
	for i := 0; i < 10; i++ {
		assert.NotNil(t, string(GetTestValue(10)))
	}
}
