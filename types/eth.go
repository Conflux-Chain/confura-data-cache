package types

import (
	"encoding/json"

	"github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// EthBlockData contains all required data in a block.
type EthBlockData struct {
	Block    *types.Block
	Receipts []types.Receipt
	Traces   []types.LocalizedTrace
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

	if block == nil {
		return EthBlockData{}, errors.Errorf("Cannot find block by number %v", blockNumber)
	}

	bnoh := types.BlockNumberOrHashWithNumber(bn)
	receipts, err := client.Parity.BlockReceipts(&bnoh)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block receipts")
	}

	if receipts == nil {
		return EthBlockData{}, errors.Errorf("Cannot find receipts by block number %v", blockNumber)
	}

	traces, err := client.Trace.Blocks(bnoh)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block traces")
	}

	if traces == nil {
		return EthBlockData{}, errors.Errorf("Cannot find traces by block number %v", blockNumber)
	}

	metrics.GetOrRegisterHistogram("types/eth/block/txs").Update(int64(len(receipts)))
	metrics.GetOrRegisterHistogram("types/eth/block/traces").Update(int64(len(traces)))

	data := EthBlockData{
		Block:    block,
		Receipts: receipts,
		Traces:   traces,
	}

	metrics.GetOrRegisterHistogram("types/eth/block/size").Update(int64(NewSized(data).Size))

	return data, nil
}

type BlockHashOrNumber struct {
	hash   *common.Hash
	number uint64
}

func BlockHashOrNumberWithHex(hex string) BlockHashOrNumber {
	hash := common.HexToHash(hex)
	return BlockHashOrNumberWithHash(hash)
}

func BlockHashOrNumberWithHash(hash common.Hash) BlockHashOrNumber {
	return BlockHashOrNumber{
		hash: &hash,
	}
}

func BlockHashOrNumberWithNumber(blockNumber uint64) BlockHashOrNumber {
	return BlockHashOrNumber{
		number: blockNumber,
	}
}

func (bhon BlockHashOrNumber) HashOrNumber() (common.Hash, bool, uint64) {
	if bhon.hash != nil {
		return *bhon.hash, true, 0
	}

	return common.Hash{}, false, bhon.number
}

func (bhon BlockHashOrNumber) MarshalJSON() ([]byte, error) {
	if bhon.hash != nil {
		return json.Marshal(*bhon.hash)
	}

	return json.Marshal(bhon.number)
}

func (bhon *BlockHashOrNumber) UnmarshalJSON(data []byte) error {
	if len(data) == 0 || data[0] != '"' {
		bhon.hash = nil
		return json.Unmarshal(data, &bhon.number)
	}

	if bhon.hash == nil {
		bhon.hash = new(common.Hash)
	}

	if err := json.Unmarshal(data, bhon.hash); err != nil {
		return err
	}

	bhon.number = 0

	return nil
}
