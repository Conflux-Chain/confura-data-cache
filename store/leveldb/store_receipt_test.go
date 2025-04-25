package leveldb

import (
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/types"
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

	receipts, err := store.GetBlockReceipts(types.BlockHashOrNumberWithHex("0x6660"))
	assert.Nil(t, err)
	assert.NotNil(t, receipts.MustLoad())
	assert.Equal(t, 0, len(receipts.MustLoad()))

	receipts, err = store.GetBlockReceipts(types.BlockHashOrNumberWithNumber(0))
	assert.Nil(t, err)
	assert.NotNil(t, receipts.MustLoad())
	assert.Equal(t, 0, len(receipts.MustLoad()))

	// write block 1 with txs
	store.Write(createTestEthData(1, common.HexToHash("0x6661"),
		common.HexToHash("0x7770"),
		common.HexToHash("0x7771"),
		common.HexToHash("0x7772"),
	))

	receipts, err = store.GetBlockReceipts(types.BlockHashOrNumberWithHex("0x6661"))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(receipts.MustLoad()))

	receipts, err = store.GetBlockReceipts(types.BlockHashOrNumberWithNumber(1))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(receipts.MustLoad()))

	// not found
	receipts, err = store.GetBlockReceipts(types.BlockHashOrNumberWithNumber(2))
	assert.Nil(t, err)
	assert.Nil(t, receipts.MustLoad())
}
