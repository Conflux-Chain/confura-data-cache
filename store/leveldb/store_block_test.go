package leveldb

import (
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestStoreGetBlock(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	store.Write(createTestEthData(0, common.HexToHash("0x6660")))
	store.Write(createTestEthData(1, common.HexToHash("0x6661")))

	// get block 0
	data, err := store.GetBlock(types.BlockHashOrNumberWithHex("0x6660"), true)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), data.MustLoad().Number.Uint64())
	assert.Equal(t, common.HexToHash("0x6660"), data.MustLoad().Hash)

	data, err = store.GetBlock(types.BlockHashOrNumberWithNumber(0), true)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), data.MustLoad().Number.Uint64())
	assert.Equal(t, common.HexToHash("0x6660"), data.MustLoad().Hash)

	// get block 1
	data, err = store.GetBlock(types.BlockHashOrNumberWithHex("0x6661"), true)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), data.MustLoad().Number.Uint64())
	assert.Equal(t, common.HexToHash("0x6661"), data.MustLoad().Hash)

	data, err = store.GetBlock(types.BlockHashOrNumberWithNumber(1), true)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), data.MustLoad().Number.Uint64())
	assert.Equal(t, common.HexToHash("0x6661"), data.MustLoad().Hash)

	// get block 2 - not found
	data, err = store.GetBlock(types.BlockHashOrNumberWithHex("0x6662"), true)
	assert.Nil(t, err)
	assert.Nil(t, data.MustLoad())

	data, err = store.GetBlock(types.BlockHashOrNumberWithNumber(2), true)
	assert.Nil(t, err)
	assert.Nil(t, data.MustLoad())
}

func TestStoreGetBlockTransactionCount(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	store.Write(createTestEthData(0, common.HexToHash("0x6660")))
	store.Write(createTestEthData(1, common.HexToHash("0x6661")))

	// block 1 without txs
	count, err := store.GetBlockTransactionCount(types.BlockHashOrNumberWithHex("0x6661"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)

	count, err = store.GetBlockTransactionCount(types.BlockHashOrNumberWithNumber(1))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)

	// write block 2 with txs
	ethData2 := createTestEthData(2, common.HexToHash("0x6662"),
		common.HexToHash("0x7770"),
		common.HexToHash("0x7771"),
		common.HexToHash("0x7772"),
	)
	assert.Nil(t, store.Write(ethData2))

	// 3 txs in block 2
	count, err = store.GetBlockTransactionCount(types.BlockHashOrNumberWithHex("0x6662"))
	assert.Nil(t, err)
	assert.Equal(t, int64(3), count)

	count, err = store.GetBlockTransactionCount(types.BlockHashOrNumberWithNumber(2))
	assert.Nil(t, err)
	assert.Equal(t, int64(3), count)

	// block 3 - not found
	count, err = store.GetBlockTransactionCount(types.BlockHashOrNumberWithHex("0x6663"))
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), count)

	count, err = store.GetBlockTransactionCount(types.BlockHashOrNumberWithNumber(3))
	assert.Nil(t, err)
	assert.Equal(t, int64(-1), count)
}
