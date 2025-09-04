package leveldb

import (
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestStoreGetTransactionTraces(t *testing.T) {
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

	traces, err := store.GetTransactionTraces(common.HexToHash("0x7771"))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(traces))
	assert.Equal(t, common.HexToHash("0x7771"), *traces[0].TransactionHash)

	// not found
	traces, err = store.GetTransactionTraces(common.HexToHash("0x7773"))
	assert.Nil(t, err)
	assert.Nil(t, traces)

	// test trace retrieval by index
	trace, err := store.GetTrace(common.HexToHash("0x7771"), 0)
	assert.Nil(t, err)
	assert.NotNil(t, trace)
	assert.Equal(t, common.HexToHash("0x7771"), *trace.TransactionHash)
}

func TestStoreGetBlockTraces(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	// write block 0
	store.Write(createTestEthData(0, common.HexToHash("0x6660")))

	traces, err := store.GetBlockTraces(types.BlockHashOrNumberWithHex("0x6660"))
	assert.Nil(t, err)
	assert.NotNil(t, traces.MustLoad())
	assert.Equal(t, 0, len(traces.MustLoad()))

	traces, err = store.GetBlockTraces(types.BlockHashOrNumberWithNumber(0))
	assert.Nil(t, err)
	assert.NotNil(t, traces.MustLoad())
	assert.Equal(t, 0, len(traces.MustLoad()))

	// write block 1 with txs
	store.Write(createTestEthData(1, common.HexToHash("0x6661"),
		common.HexToHash("0x7770"),
		common.HexToHash("0x7771"),
		common.HexToHash("0x7772"),
	))

	traces, err = store.GetBlockTraces(types.BlockHashOrNumberWithHex("0x6661"))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(traces.MustLoad()))

	traces, err = store.GetBlockTraces(types.BlockHashOrNumberWithNumber(1))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(traces.MustLoad()))

	// not found
	traces, err = store.GetBlockTraces(types.BlockHashOrNumberWithNumber(2))
	assert.Nil(t, err)
	assert.Nil(t, traces.MustLoad())
}
