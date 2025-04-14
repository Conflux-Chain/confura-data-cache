package extract_test

import (
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/stretchr/testify/assert"
)

func TestBlockHashWindow(t *testing.T) {
	w := extract.NewBlockHashWindow[string](2)

	// Test adding elements
	err := w.Push(1, "hash1")
	assert.NoError(t, err)
	assert.Equal(t, 1, w.Len())

	err = w.Push(2, "hash2")
	assert.NoError(t, err)
	assert.Equal(t, 2, w.Len())

	// Test Peek
	blockNum, blockHash, ok := w.Peek()
	assert.True(t, ok && blockNum == 2 && blockHash == "hash2")

	// Test non-sequential fails
	err = w.Push(4, "hash4")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not continuous")

	// Test ring buffer overwrite
	err = w.Push(3, "hash3")
	assert.NoError(t, err)
	assert.Equal(t, 2, w.Len())

	// Test Pop
	blockNum, hash := w.Pop()
	assert.True(t, blockNum == 3 && hash == "hash3")

	blockNum, hash = w.Pop()
	assert.True(t, blockNum == 2 && hash == "hash2")

	assert.Equal(t, 0, w.Len())

	// Test empty Pop
	blockNum, hash = w.Pop()
	assert.True(t, blockNum == 0 && hash == "")

	// Test Peek on empty
	blockNum, hash, ok = w.Peek()
	assert.True(t, !ok && blockNum == 0 && hash == "")
}
