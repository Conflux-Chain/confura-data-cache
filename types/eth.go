package types

import (
	"encoding/json"
	"regexp"
	"strings"
	"sync/atomic"

	"github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var (
	// ethTraceRpcKnownSupported is an atomic value which stores whether eth trace RPC is supported.
	// - If nil: The support status of eth trace RPC is unknown.
	// - If non-nil (and stores true): eth trace RPC is known to be supported.
	// - If non-nil (and stores false): eth trace RPC is known to be unsupported.
	ethTraceRpcKnownSupported atomic.Value

	errTraceRpcNotSupported  = errors.New("trace RPC not supported")
	rpcMethodNotFoundPattern = regexp.MustCompile(`method.*(?:not found|not exist|not available)`)
)

// isRpcMethodNotSupportedError checks whether the error indicates an unsupported RPC method.
func isRpcMethodNotSupportedError(err error) bool {
	if err == nil {
		return false
	}
	return rpcMethodNotFoundPattern.MatchString(strings.ToLower(err.Error()))
}

// EthBlockData contains all required data in a block.
type EthBlockData struct {
	Block    *types.Block
	Receipts []*types.Receipt
	Traces   []types.LocalizedTrace
}

func (d *EthBlockData) BlockSummary() *types.Block {
	block := *d.Block

	var hashes []common.Hash

	if txs := block.Transactions.Transactions(); txs != nil {
		hashes = make([]common.Hash, 0, len(txs))

		for _, tx := range txs {
			hashes = append(hashes, tx.Hash)
		}
	}

	block.Transactions = *types.NewTxOrHashListByHashes(hashes)

	return &block
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

	blockTxs := block.Transactions.Transactions()

	// If the block has no transactions, there is no need to query receipts or traces.
	if len(blockTxs) == 0 {
		return EthBlockData{
			Block:    block,
			Receipts: []*types.Receipt{},
			Traces:   []types.LocalizedTrace{},
		}, nil
	}

	bnoh := types.BlockNumberOrHashWithNumber(bn)

	receipts, err := QueryEthBlockReceipts(client, bnoh)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block receipts")
	}

	// Validate receipts
	if len(receipts) != len(blockTxs) {
		return EthBlockData{}, errors.Errorf(
			"Transaction/receipt count mismatch for block %v: block has %d transactions, but received %d receipts",
			blockNumber, len(blockTxs), len(receipts),
		)
	}
	for _, rcpt := range receipts {
		if rcpt.BlockHash != block.Hash {
			return EthBlockData{}, errors.Errorf(
				"Receipt block hash mismatch for block %v: receipt has %s, expected %s",
				blockNumber, rcpt.BlockHash, block.Hash,
			)
		}
	}

	traces, err := QueryEthBlockTraces(client, bnoh)
	if err != nil && !errors.Is(err, errTraceRpcNotSupported) {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block traces")
	}

	// Validate traces
	for i := range traces {
		if traces[i].BlockHash != block.Hash {
			return EthBlockData{}, errors.Errorf(
				"Trace block hash mismatch for block %v: trace references %s, expected %s",
				blockNumber, traces[i].BlockHash, block.Hash,
			)
		}
	}

	data := EthBlockData{
		Block:    block,
		Receipts: receipts,
		Traces:   traces,
	}

	metrics.GetOrRegisterHistogram("types/eth/block/txs").Update(int64(len(receipts)))
	metrics.GetOrRegisterHistogram("types/eth/block/traces").Update(int64(len(traces)))
	metrics.GetOrRegisterHistogram("types/eth/block/size").Update(int64(NewSized(data).Size))

	return data, nil
}

func QueryEthBlockReceipts(client *web3go.Client, bnoh types.BlockNumberOrHash) ([]*types.Receipt, error) {
	receipts, err := client.Eth.BlockReceipts(&bnoh)
	if err != nil {
		return nil, err
	}

	// Some RPC providers return nil if the block is not found instead of an error
	if receipts == nil {
		return nil, errors.Errorf("Cannot find receipts by block %v", bnoh)
	}

	return receipts, nil
}

func QueryEthBlockTraces(client *web3go.Client, bnoh types.BlockNumberOrHash) ([]types.LocalizedTrace, error) {
	traceRpcSuppported, confirmed := ethTraceRpcKnownSupported.Load().(bool)
	if confirmed && !traceRpcSuppported {
		return nil, errTraceRpcNotSupported
	}

	traces, err := client.Trace.Blocks(bnoh)
	if err == nil {
		ethTraceRpcKnownSupported.Store(true)

		// Some RPC providers return nil if the block is not found instead of an error
		if traces == nil {
			return nil, errors.Errorf("Cannot find traces by block %v", bnoh)
		}
		return traces, nil
	}

	if !confirmed && isRpcMethodNotSupportedError(err) {
		ethTraceRpcKnownSupported.Store(false)
		return nil, errTraceRpcNotSupported
	}

	return nil, err
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
