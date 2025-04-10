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

func createTestStore(t *testing.T) (*Store, func()) {
	path, err := os.MkdirTemp("", "confura-data-cache-")
	assert.Nil(t, err)

	store, err := NewStore(path)
	assert.Nil(t, err)

	return store, func() {
		store.Close()
		os.RemoveAll(path)
	}
}

func createTestEthData(blockNumber int64, blockHash common.Hash, txHashes ...common.Hash) types.EthBlockData {
	var txs []ethTypes.TransactionDetail
	for _, v := range txHashes {
		txs = append(txs, ethTypes.TransactionDetail{
			Hash:        v,
			BlockNumber: big.NewInt(blockNumber),
			BlockHash:   &blockHash,
		})
	}

	return types.EthBlockData{
		Block: &ethTypes.Block{
			Number:       big.NewInt(blockNumber),
			Hash:         blockHash,
			Difficulty:   big.NewInt(999),
			Transactions: *ethTypes.NewTxOrHashListByTxs(txs),
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

	// check next block number
	next, ok, err := store.getNextBlockNumber()
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), next)

	assert.Equal(t, uint64(3), store.NextBlockNumber())
}
