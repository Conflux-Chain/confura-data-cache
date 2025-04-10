package sync

import (
	"context"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	ErrInconsistentChainData = errors.New("inconsistent chain data")
)

func NewInconsistentChainDataError(reason string) error {
	return errors.WithMessagef(ErrInconsistentChainData, reason)
}

// QueryEthData queries evm-compatible blockchain data for the specified block number.
func QueryEthData(ctx context.Context, client *web3go.Client, blockNumber uint64) (*types.EthBlockData, error) {
	// Retrieve the block by number
	rpcBlockNumber := ethTypes.BlockNumber(blockNumber)
	block, err := client.Eth.BlockByNumber(rpcBlockNumber, true)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get block by number %v", blockNumber)
	}
	if block == nil {
		return nil, errors.Errorf("unknown block for number `%v`", blockNumber)
	}

	// Retrieve block receipts
	bnh := ethTypes.BlockNumberOrHashWithNumber(rpcBlockNumber)
	receipts, err := client.WithContext(ctx).Eth.BlockReceipts(&bnh)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get eth block receipts")
	}

	// Retrive block traces
	traces, err := client.WithContext(ctx).Trace.Blocks(bnh)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to get block traces")
	}

	// Verify the consistency of the block data
	blockData := &types.EthBlockData{
		Block:    block,
		Receipts: receipts,
		Traces:   traces,
	}
	if err := VerifyEthBlockData(blockData); err != nil {
		return nil, NewInconsistentChainDataError(err.Error())
	}
	return blockData, nil
}

func VerifyEthBlockData(data *types.EthBlockData) error {
	block, receipts, traces := data.Block, data.Receipts, data.Traces
	// Ensure the number of receipts matches the number of transactions
	if txnCnt := len(block.Transactions.Transactions()); len(receipts) != txnCnt {
		return errors.Errorf(
			"mismatched number of transactions and receipts: have %d transactions, got %d receipts",
			txnCnt, len(receipts),
		)
	}

	// Ensure data consistency for the block receipts
	for i, tx := range block.Transactions.Transactions() {
		if receipts[i] == nil {
			return errors.Errorf("nil receipt for txn (%v) in block (%v)",
				tx.Hash, block.Hash,
			)
		}
		if receipts[i].BlockHash != block.Hash {
			return errors.Errorf(
				"block hash mismatch: receipt has %v, expected %v",
				receipts[i].BlockHash, block.Hash,
			)
		}
		if tx.Hash != receipts[i].TransactionHash {
			return errors.Errorf(
				"txn hash mismatch: receipt has %v, expected %v in block %v",
				receipts[i].TransactionHash, tx.Hash, block.Hash,
			)
		}
	}

	// Ensure data consistency for the block traces
	// TODO: It's hard to ensure the consistency of the traces unless full node provides a method to query
	// the trace by block hash.
	for _, trace := range traces {
		if trace.BlockHash != block.Hash {
			return errors.Errorf(
				"block hash mismatch: trace has %v, expected %v",
				trace.BlockHash, block.Hash,
			)
		}
	}

	return nil
}
