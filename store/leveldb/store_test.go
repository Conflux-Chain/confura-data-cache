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

func createTestStore(t *testing.T, defaultNextBlockNumber ...uint64) (*Store, func()) {
	path, err := os.MkdirTemp("", "confura-data-cache-")
	assert.Nil(t, err)

	config := Config{
		Path: path,
	}
	if len(defaultNextBlockNumber) > 0 {
		config.DefaultNextBlockNumber = defaultNextBlockNumber[0]
	}

	store, err := NewStore(config)
	assert.Nil(t, err)

	return store, func() {
		store.Close()
		os.RemoveAll(path)
	}
}

func createTestEthData(blockNumber int64, blockHash common.Hash, txHashes ...common.Hash) types.EthBlockData {
	// fullnode return empty array instead of nil for following data
	txs := make([]ethTypes.TransactionDetail, 0, len(txHashes))
	receipts := make([]*ethTypes.Receipt, 0, len(txHashes))
	traces := make([]ethTypes.LocalizedTrace, 0)

	for i, v := range txHashes {
		txs = append(txs, ethTypes.TransactionDetail{
			BlockHash:   &blockHash,
			BlockNumber: big.NewInt(blockNumber),
			Hash:        v,
			Gas:         21000,
			Input:       []byte{},
			Nonce:       666,
			R:           big.NewInt(1),
			S:           big.NewInt(2),
			V:           big.NewInt(3),
			Value:       big.NewInt(4),
		})

		receipts = append(receipts, &ethTypes.Receipt{
			BlockHash:        blockHash,
			BlockNumber:      uint64(blockNumber),
			TransactionHash:  v,
			TransactionIndex: uint64(i),
		})

		traceTxIndex := uint(i)
		traces = append(traces, ethTypes.LocalizedTrace{
			Type:                ethTypes.TRACE_CALL,
			BlockHash:           blockHash,
			BlockNumber:         uint64(blockNumber),
			TransactionHash:     &v,
			TransactionPosition: &traceTxIndex,
		})
	}

	return types.EthBlockData{
		Block: &ethTypes.Block{
			Number:       big.NewInt(blockNumber),
			Hash:         blockHash,
			Difficulty:   big.NewInt(999),
			Transactions: *ethTypes.NewTxOrHashListByTxs(txs),
		},
		Receipts: receipts,
		Traces:   traces,
	}
}

func TestStoreWrite(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	earlist, ok := store.EarlistBlockNumber()
	assert.Equal(t, uint64(0), earlist)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), store.NextBlockNumber())

	// write empty
	assert.Nil(t, store.Write())

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

	// check earlist block number
	earlist, ok, err := store.readUint64(keyEarlistBlockNumber)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(0), earlist)

	earlist, ok = store.EarlistBlockNumber()
	assert.Equal(t, uint64(0), earlist)
	assert.True(t, ok)

	// check next block number
	next, ok, err := store.readUint64(keyNextBlockNumber)
	assert.Nil(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(3), next)

	assert.Equal(t, uint64(3), store.NextBlockNumber())
}

func TestStoreBreakPoint(t *testing.T) {
	path, err := os.MkdirTemp("", "confura-data-cache-")
	assert.Nil(t, err)

	store, err := NewStore(Config{path, 7})
	assert.Nil(t, err)
	assert.Equal(t, uint64(7), store.NextBlockNumber())

	// write block 7 & 8
	store.Write(createTestEthData(7, common.HexToHash("0x6667")))
	store.Write(createTestEthData(8, common.HexToHash("0x6668")))

	// close store
	assert.Nil(t, store.Close())

	// reopen db again
	store, err = NewStore(Config{path, 0})
	assert.Nil(t, err)

	// earlist block is 7
	earlist, ok := store.EarlistBlockNumber()
	assert.True(t, ok)
	assert.Equal(t, uint64(7), earlist)

	// start from block 2
	assert.Equal(t, uint64(9), store.NextBlockNumber())
}
