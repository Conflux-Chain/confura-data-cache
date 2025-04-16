package extract_test

import (
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestBlockHashCache(t *testing.T) {
	// Cache without provider
	cwp := extract.NewBlockHashCache(2)

	// Test appending elements
	err := cwp.Append(1, common.HexToHash("0x1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, cwp.Len())

	err = cwp.Append(2, common.HexToHash("0x2"))
	assert.NoError(t, err)
	assert.Equal(t, 2, cwp.Len())

	// Test getting latest element
	blockNum, blockHash, ok := cwp.Latest()
	assert.True(t, ok && blockNum == 2 && blockHash == common.HexToHash("0x2"))

	// Test non-sequential fails
	err = cwp.Append(4, common.HexToHash("0x2"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not continuous")

	// Test eviction
	err = cwp.Append(3, common.HexToHash("0x3"))
	assert.NoError(t, err)
	assert.Equal(t, 2, cwp.Len())

	// Test popping elements
	blockNum, hash, ok := cwp.Pop()
	assert.True(t, ok && blockNum == 3 && hash == common.HexToHash("0x3"))

	// Test appending after pop
	err = cwp.Append(4, common.HexToHash("0x4"))
	assert.Contains(t, err.Error(), "not continuous")

	blockNum, hash, ok = cwp.Pop()
	assert.True(t, ok && blockNum == 2 && hash == common.HexToHash("0x2"))

	assert.Equal(t, 0, cwp.Len())

	// Test empty popping
	blockNum, hash, ok = cwp.Pop()
	assert.True(t, !ok && blockNum == 0 && hash == common.Hash{})

	// Cache with provider
	cp := extract.NewBlockHashCacheWithProvider(func() (uint64, error) {
		return 2, nil
	})

	// Test appending elements
	err = cp.Append(1, common.HexToHash("0x1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, cp.Len())

	err = cp.Append(2, common.HexToHash("0x2"))
	assert.NoError(t, err)
	assert.Equal(t, 1, cp.Len())

	err = cp.Append(2, common.HexToHash("0x3"))
	assert.Contains(t, err.Error(), "not continuous")
}
