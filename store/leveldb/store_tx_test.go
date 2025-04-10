package leveldb

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestStoreGetTransaction(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	// write block 0
	store.Write(createTestEthData(0, common.HexToHash("0x6660")))

	// write block 1 with txs
	store.Write(createTestEthData(1, common.HexToHash("0x6661"),
		common.HexToHash("0x7770"),
		common.HexToHash("0x7771"),
		common.HexToHash("0x7772"),
	))

	// get tx by hash
	tx, err := store.GetTransactionByHash(common.HexToHash("0x7771"))
	assert.Nil(t, err)
	assert.Equal(t, common.HexToHash("0x7771"), tx.Hash)
	assert.Equal(t, uint64(1), tx.BlockNumber.Uint64())
	assert.Equal(t, common.HexToHash("0x6661"), *tx.BlockHash)

	// get tx by block hash and index
	tx, err = store.GetTransactionByBlockHashAndIndex(common.HexToHash("0x6661"), 2)
	assert.Nil(t, err)
	assert.Equal(t, common.HexToHash("0x7772"), tx.Hash)
	assert.Equal(t, uint64(1), tx.BlockNumber.Uint64())
	assert.Equal(t, common.HexToHash("0x6661"), *tx.BlockHash)

	// get tx by block number and index
	tx, err = store.GetTransactionByBlockNumberAndIndex(1, 0)
	assert.Nil(t, err)
	assert.Equal(t, common.HexToHash("0x7770"), tx.Hash)
	assert.Equal(t, uint64(1), tx.BlockNumber.Uint64())
	assert.Equal(t, common.HexToHash("0x6661"), *tx.BlockHash)

	// tx not found
	tx, err = store.GetTransactionByHash(common.HexToHash("0x7773"))
	assert.Nil(t, err)
	assert.Nil(t, tx)
}
