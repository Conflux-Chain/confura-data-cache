package types

import (
	"sync/atomic"

	"github.com/DmitriyVTitov/size"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// EthBlockData contains all required data in a block.
type EthBlockData struct {
	Block    *types.Block
	Receipts []types.Receipt
	Traces   []types.LocalizedTrace
	size     atomic.Uint64 // cached size in bytes
}

func (d *EthBlockData) Size() (v uint64) {
	if v = d.size.Load(); v == 0 {
		v = uint64(size.Of(d))
		d.size.Store(v)
	}
	return
}

func (d *EthBlockData) Verify() error {
	block, receipts, traces := d.Block, d.Receipts, d.Traces

	// Ensure the number of receipts matches the number of transactions in the block body.
	if txnCnt := len(block.Transactions.Transactions()); len(receipts) != txnCnt {
		return errors.Errorf(
			"transaction/receipt count mismatch for block %s: block body has %d transactions, but received %d receipts",
			block.Hash, txnCnt, len(receipts),
		)
	}

	// Check each receipt belongs to this block and corresponds to the correct transaction.
	for i, tx := range block.Transactions.Transactions() {
		receipt := receipts[i]

		// Check if the receipt's BlockHash matches the actual block's hash
		if receipt.BlockHash != block.Hash {
			return errors.Errorf(
				"receipt %d (Tx: %s) block hash mismatch: receipt has %s, expected block %s",
				i, tx.Hash, receipt.BlockHash, block.Hash,
			)
		}

		// Check if the receipt's TransactionHash matches the actual transaction's hash
		if receipt.TransactionHash != tx.Hash {
			return errors.Errorf(
				"receipt #%d transaction hash mismatch: receipt has tx %s, expected tx %s in block %s",
				i, receipt.TransactionHash, tx.Hash, block.Hash,
			)
		}

		// Check TxIndex consistency
		if receipt.TransactionIndex != uint64(i) {
			return errors.Errorf(
				"receipt #%d transaction index mismatch: receipt has %d, expected %d in block %s",
				i, receipt.TransactionIndex, i, block.Hash,
			)
		}
	}

	for i, trace := range traces {
		// Check if the trace's BlockHash matches the actual block's hash
		if trace.BlockHash != block.Hash {
			return errors.Errorf(
				"trace #%d block hash mismatch: trace references %s, expected block %s",
				i, trace.BlockHash, block.Hash,
			)
		}
	}

	return nil
}

func QueryEthBlockData(client *web3go.Client, blockNumber uint64) (EthBlockData, error) {
	bn := types.NewBlockNumber(int64(blockNumber))
	block, err := client.Eth.BlockByNumber(bn, true)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block by number")
	}

	bnoh := types.BlockNumberOrHashWithNumber(bn)
	receipts, err := client.Parity.BlockReceipts(&bnoh)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block receipts")
	}

	traces, err := client.Trace.Blocks(bnoh)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block traces")
	}

	return EthBlockData{
		Block:    block,
		Receipts: receipts,
		Traces:   traces,
	}, nil
}
