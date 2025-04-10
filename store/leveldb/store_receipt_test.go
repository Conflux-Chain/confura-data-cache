package leveldb

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestStoreGetReceipt(t *testing.T) {
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

	// get receipt by hash
	receipt, err := store.GetTransactionReceipt(common.HexToHash("0x7771"))
	assert.Nil(t, err)
	assert.Equal(t, common.HexToHash("0x7771"), receipt.TransactionHash)

	// not found
	receipt, err = store.GetTransactionReceipt(common.HexToHash("0x7773"))
	assert.Nil(t, err)
	assert.Nil(t, receipt)
}

func TestStoreGetBlockReceipts(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	// write block 0
	store.Write(createTestEthData(0, common.HexToHash("0x6660")))

	receipts, err := store.GetBlockReceiptsByHash(common.HexToHash("0x6660"))
	assert.Nil(t, err)
	assert.Nil(t, receipts)

	receipts, err = store.GetBlockReceiptsByNumber(0)
	assert.Nil(t, err)
	assert.Nil(t, receipts)

	// write block 1 with txs
	store.Write(createTestEthData(1, common.HexToHash("0x6661"),
		common.HexToHash("0x7770"),
		common.HexToHash("0x7771"),
		common.HexToHash("0x7772"),
	))

	receipts, err = store.GetBlockReceiptsByHash(common.HexToHash("0x6661"))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(receipts))

	receipts, err = store.GetBlockReceiptsByNumber(1)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(receipts))

	// not found
	receipts, err = store.GetBlockReceiptsByNumber(2)
	assert.Nil(t, err)
	assert.Nil(t, receipts)
}
