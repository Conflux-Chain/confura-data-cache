package extract

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
)

const (
	// normalPollInterval specifies how often to poll for new blocks once the extractor
	// is synchronized. This is the polling rate during normal, steady-state operation.
	normalPollInterval time.Duration = 1 * time.Second

	// catchupPollInterval specifies how often to poll for new blocks while the
	// extractor is behind the target block number. This faster polling rate helps
	// expedite the initial synchronization process.
	catchupPollInterval time.Duration = 1 * time.Millisecond
)

// EthExtractor is an EVM compatible blockchain data extractor.
// TODO:
// 1. add pivot reorg check to ensure data consistency
// 2. add catch-up sync to allow concurrent syncing of multiple blocks
// 3. add metrics, logging and monitoring
// 4. add graceful shutdown to ensure data consistency
// 5. add memory threshold limit to avoid OOM
type EthExtractor struct {
	EthConfig
	rpcClient *web3go.Client // Connected RPC clients for fetching blockchain data
}

func NewEvmExtractor(conf EthConfig) (*EthExtractor, error) {
	if len(conf.RpcEndpoint) == 0 {
		return nil, errors.New("no rpc endpoint provided")
	}

	client, err := web3go.NewClient(conf.RpcEndpoint)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to create rpc client for endpoint %s", conf.RpcEndpoint)
	}

	return &EthExtractor{
		EthConfig: conf,
		rpcClient: client,
	}, nil
}

func (e *EthExtractor) Start(ctx context.Context, dataChan chan<- types.EthBlockData) {
	defer e.rpcClient.Close()

	ticker := time.NewTimer(catchupPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data, caughtup, err := e.extractOnce()
			if err == nil {
				// If the extraction was successful, collect the results to the result channel
				// TODO: limit the memory usage of the result slice
				dataChan <- *data
			}

			// Reset the timer based on the result of the extraction
			// If the extraction was successful and aligned, use the aligned poll interval
			// Otherwise, use the catchup poll interval
			if err != nil || caughtup {
				ticker.Reset(normalPollInterval)
			} else {
				ticker.Reset(catchupPollInterval)
			}
		}
	}
}

// extractOnce fetches the block data for the current block number.
func (e *EthExtractor) extractOnce() (*types.EthBlockData, bool, error) {
	// Get the alignment block header to determine the actual target block number to synchronize against.
	targetBlock, err := e.rpcClient.Eth.BlockByNumber(e.TargetBlockNumber, false)
	if err != nil {
		return nil, false, errors.WithMessage(err, "failed to get target block heander")
	}
	if targetBlock == nil { // Should not happen but defensive check.
		return nil, false, errors.Errorf("target block %v not found or invalid", e.TargetBlockNumber)
	}

	// Check if we are already caught up.
	if e.StartBlockNumber > targetBlock.Number.Uint64() {
		return nil, true, nil
	}

	// Fetch the block data for the current block number.
	blockData, err := types.QueryEthBlockData(e.rpcClient, e.StartBlockNumber)
	if err != nil {
		return nil, false, errors.WithMessage(err, "failed to fetch block data")
	}

	// Validate the block data
	if err := e.validateBlockData(blockData); err != nil {
		return nil, false, errors.WithMessage(err, "failed to validate block data")
	}

	e.StartBlockNumber++
	return &blockData, false, nil
}

func (e *EthExtractor) validateBlockData(data types.EthBlockData) error {
	block, receipts, traces := data.Block, data.Receipts, data.Traces

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
				"receipt %d transaction hash mismatch: receipt has tx %s, expected tx %s in block %s",
				i, receipt.TransactionHash, tx.Hash, block.Hash,
			)
		}

		// Check TxIndex consistency
		if receipt.TransactionIndex != uint64(i) {
			return errors.Errorf(
				"receipt %d transaction index mismatch: receipt has %d, expected %d in block %s",
				i, receipt.TransactionIndex, i, block.Hash,
			)
		}
	}

	// Validate each trace belongs to this block.
	for i, trace := range traces {
		// Check if the trace's BlockHash matches the actual block's hash
		if trace.BlockHash != block.Hash {
			return errors.Errorf(
				"trace %d block hash mismatch: trace references %s, expected block %s",
				i, trace.BlockHash, block.Hash,
			)
		}
	}

	return nil
}
