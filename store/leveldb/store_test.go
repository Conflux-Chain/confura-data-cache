package leveldb

import (
	"math/big"
	"os"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/stretchr/testify/assert"
)

func createTestStore(t *testing.T) (*Store[types.EthBlockData], func()) {
	path, err := os.MkdirTemp("", "confura-data-cache-")
	assert.Nil(t, err)

	store, err := NewStore[types.EthBlockData](path)
	assert.Nil(t, err)

	return store, func() {
		store.Close()
		os.RemoveAll(path)
	}
}

func createTestEthData(blockNumber int64, blockHash common.Hash) types.EthBlockData {
	return types.EthBlockData{
		Block: &ethTypes.Block{
			Number:     big.NewInt(blockNumber),
			Hash:       blockHash,
			Difficulty: big.NewInt(999),
		},
	}
}

func TestStoreWrite(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	assert.Equal(t, uint64(0), store.NextBlockNumber())

	// write block 0
	ethData0 := createTestEthData(0, common.HexToHash("0x6660"))
	assert.Nil(t, store.Write(ethData0))

	// write block 1
	ethData1 := createTestEthData(1, common.HexToHash("0x6661"))
	assert.Nil(t, store.Write(ethData1))

	// cannot write block 1 again
	assert.Error(t, store.Write(ethData1))

	// cannot write block 3
	ethData3 := createTestEthData(3, common.HexToHash("0x6663"))
	assert.Error(t, store.Write(ethData3))

	// write block 2
	ethData2 := createTestEthData(2, common.HexToHash("0x6662"))
	assert.Nil(t, store.Write(ethData2))

	// check next block number in database
	next, ok, err := store.getNextBlockNumber()
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), next)
}

func TestStoreGetBlock(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	// write block 0
	ethData0 := createTestEthData(0, common.HexToHash("0x6660"))
	assert.Nil(t, store.Write(ethData0))

	// write block 1
	ethData1 := createTestEthData(1, common.HexToHash("0x6661"))
	assert.Nil(t, store.Write(ethData1))

	// get block by hash - found
	data, err := store.GetBlockByHash(common.HexToHash("0x6660"))
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), data.Number.Uint64())
	assert.Equal(t, common.HexToHash("0x6660"), data.Hash)

	// get block by hash - not found
	data, err = store.GetBlockByHash(common.HexToHash("0x8880"))
	assert.Nil(t, err)
	assert.Nil(t, data)

	// get block by number - found
	data, err = store.GetBlockByNumber(1)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), data.Number.Uint64())
	assert.Equal(t, common.HexToHash("0x6661"), data.Hash)

	// get block by number - not found
	data, err = store.GetBlockByNumber(33)
	assert.Nil(t, err)
	assert.Nil(t, data)
}
