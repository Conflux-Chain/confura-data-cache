package sync_test

import (
	"context"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/sync"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	"github.com/stretchr/testify/assert"
)

func TestQueryEthData(t *testing.T) {
	client, err := web3go.NewClient("http://evmtestnet.confluxrpc.com")
	assert.NoError(t, err)

	blockData, err := sync.QueryEthData(context.Background(), client, 212929075)
	assert.NoError(t, err)
	assert.NotNil(t, blockData)

	// Validate the block data
	block, receipts, traces := blockData.Block, blockData.Receipts, blockData.Traces
	assert.Equal(t, len(block.Transactions.Transactions()), len(receipts))

	if len(traces) > 0 {
		assert.Equal(t, traces[0].BlockHash, block.Hash)
		assert.Equal(t, traces[0].BlockNumber, block.Number.Uint64())
		txnPos := traces[0].TransactionPosition
		assert.NotNil(t, txnPos)
		assert.Equal(t, *traces[0].TransactionHash, block.Transactions.Transactions()[*txnPos].Hash)
	}

	// Test the consistency of the block data
	block.Hash = common.Hash{}
	assert.Error(t, sync.VerifyEthBlockData(blockData))
}
