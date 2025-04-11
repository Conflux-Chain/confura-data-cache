package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/mcuadros/go-defaults"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// EvmExtractOptions is the options for extracting EVM compatible blockchain data.
type EvmExtractOptions struct {
	ExtractOptions

	AlignBlockNumber rpc.BlockNumber
}

// EvmExtractor is an EVM compatible blockchain data extractor.
// TODO:
// 1. add pivot reorg check to ensure data consistency
// 2. add catch-up sync to allow concurrent syncing of multiple blocks
// 3. add metrics, logging and monitoring
// 4. add graceful shutdown to ensure data consistency
// 5. add memory threshold limit to avoid OOM
type EvmExtractor struct {
	Config

	running    atomic.Bool      // Indicates if the extractor is actively syncing
	cancel     atomic.Value     // Context cancellation function to stop syncing
	clientIdx  atomic.Uint32    // Index of the selected RPC client
	rpcClients []*web3go.Client // Connected RPC clients for fetching blockchain data
}

func NewEvmExtractor(conf Config) (*EvmExtractor, error) {
	if len(conf.RpcEndpoints) == 0 {
		return nil, errors.New("no rpc endpoints provided")
	}

	var clients []*web3go.Client
	for _, endpoint := range conf.RpcEndpoints {
		client, err := web3go.NewClient(endpoint)
		if err != nil {
			return nil, errors.WithMessagef(err, "failed to create rpc client for endpoint %s", endpoint)
		}
		clients = append(clients, client)
	}

	return &EvmExtractor{
		Config:     conf,
		rpcClients: clients,
	}, nil
}

func (e *EvmExtractor) Subscribe(ctx context.Context, opts ...ExtractOptions) (<-chan types.EthBlockData, error) {
	if !e.running.CompareAndSwap(false, true) {
		return nil, errors.New("extractor is already running")
	}

	var opt ExtractOptions
	defaults.SetDefaults(&opt)
	if len(opts) > 0 {
		opt = opts[0]
	}

	alignBlockNumber := rpc.LatestBlockNumber
	if len(opt.AlignBlockTag) > 0 {
		opt.AlignBlockTag = fmt.Sprintf(`"%v"`, opt.AlignBlockTag)
		if err := json.Unmarshal([]byte(opt.AlignBlockTag), &alignBlockNumber); err != nil {
			e.running.Store(false)
			return nil, errors.WithMessagef(err, "failed to parse align block tag %v", opt.AlignBlockTag)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	e.cancel.Store(cancel)

	resultChan := make(chan types.EthBlockData, e.ResultBufferSize)
	go e.run(ctx, resultChan, EvmExtractOptions{opt, alignBlockNumber})

	return resultChan, nil
}

func (e *EvmExtractor) run(ctx context.Context, resultChan chan<- types.EthBlockData, opt EvmExtractOptions) {
	defer close(resultChan)
	defer e.running.Store(false)

	ticker := time.NewTimer(opt.CatchupPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, aligned, err := e.extractOnce(ctx, opt)
			if err == nil {
				// If the extraction was successful, collect the results to the result channel
				// TODO: limit the memory usage of the result slice
				for _, data := range result {
					resultChan <- data
				}
			}

			// Reset the timer based on the result of the extraction
			// If the extraction was successful and aligned, use the aligned poll interval
			// Otherwise, use the catchup poll interval
			if err != nil || aligned {
				ticker.Reset(opt.AlignedPollInterval)
			} else {
				ticker.Reset(opt.CatchupPollInterval)
			}
		}
	}
}

// extractOnce attempts to fetch a single batch of Ethereum block data.
// It fetches blocks starting from e.StartBlockNumber up to a maximum of e.MaxBatchSize blocks,
// but not exceeding the target block specified by opt.AlignBlockNumber.
func (e *EvmExtractor) extractOnce(ctx context.Context, opt EvmExtractOptions) ([]types.EthBlockData, bool, error) {
	// TODO: Implement robust RPC client selection (e.g., round-robin, health checks) for high availability.
	// For now, using the currently selected client index.
	client := e.rpcClients[e.clientIdx.Load()]

	// Get the alignment block header to determine the actual target block number to synchronize against.
	alignBlockHeader, err := client.Eth.BlockByNumber(opt.AlignBlockNumber, false)
	if err != nil {
		return nil, false, errors.WithMessagef(err, "failed to get the align block %v", opt.AlignBlockNumber)
	}
	if alignBlockHeader == nil { // Should not happen but defensive check.
		return nil, false, errors.Errorf("alignment block %v not found or invalid", opt.AlignBlockNumber)
	}

	// Check if we are already aligned (caught up).
	alignBlockNumber := alignBlockHeader.Number.Uint64()
	if e.StartBlockNumber > alignBlockNumber {
		return nil, true, nil
	}

	// Calculate the range of blocks for this batch.
	toBlockNumber := e.StartBlockNumber + uint64(e.MaxBatchSize) - 1
	if toBlockNumber > alignBlockNumber {
		toBlockNumber = alignBlockNumber
	}

	// Fetch block data for the calculated range.
	numBlocksToFetch := int(toBlockNumber - e.StartBlockNumber + 1)
	batchBlockData := make([]types.EthBlockData, 0, numBlocksToFetch)
	for blockNum := e.StartBlockNumber; blockNum <= toBlockNumber; blockNum++ {
		blockData, err := e.fetchOneBlock(ctx, client, blockNum)
		if err != nil {
			// Return partial batch? Or fail the whole batch? Failing whole batch is usually simpler.
			return nil, false, errors.Wrapf(err, "failed to fetch block data for block number %d", blockNum)
		}
		if blockData == nil {
			// It should ideally return an error if data is nil, but handle defensively.
			return nil, false, errors.Errorf("received nil block data for block number %d", blockNum)
		}
		batchBlockData = append(batchBlockData, *blockData)
	}

	// Update the start block number for the next batch
	e.StartBlockNumber = toBlockNumber + 1
	return batchBlockData, false, nil
}

func (e *EvmExtractor) fetchOneBlock(
	ctx context.Context, client *web3go.Client, blockNumber uint64) (*types.EthBlockData, error) {
	// Retrieve the block by number
	rpcBlockNumber := ethTypes.BlockNumber(blockNumber)
	block, err := client.Eth.BlockByNumber(rpcBlockNumber, true)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get block %v", rpcBlockNumber)
	}
	if block == nil { // Should not happen but defensive check.
		return nil, errors.Errorf("block %v not found or invalid", rpcBlockNumber)
	}

	// Retrieve block receipts
	bnh := ethTypes.BlockNumberOrHashWithNumber(rpcBlockNumber)
	receipts, err := client.WithContext(ctx).Eth.BlockReceipts(&bnh)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get receipts for block %v", rpcBlockNumber)
	}

	// Retrive block traces
	traces, err := client.WithContext(ctx).Trace.Blocks(bnh)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to get traces for block %v", rpcBlockNumber)
	}

	// Verify the block data
	blockData := &types.EthBlockData{
		Block:    block,
		Receipts: receipts,
		Traces:   traces,
	}
	if err := e.verifyBlockData(blockData); err != nil {
		return nil, NewInconsistentChainDataError(err.Error())
	}
	return blockData, nil
}

func (e *EvmExtractor) verifyBlockData(data *types.EthBlockData) error {
	block, receipts, traces := data.Block, data.Receipts, data.Traces

	if block == nil || receipts == nil {
		return errors.New("incomplete data provided for verification")
	}

	// Ensure the number of receipts matches the number of transactions in the block body.
	if txnCnt := len(block.Transactions.Transactions()); len(receipts) != txnCnt {
		return errors.Errorf(
			"transaction/receipt count mismatch for block %s: block body has %d transactions, but received %d receipts",
			block.Hash, txnCnt, len(receipts),
		)
	}

	// Verify each receipt belongs to this block and corresponds to the correct transaction.
	for i, tx := range block.Transactions.Transactions() {
		receipt := receipts[i]

		// Check for nil receipt entry (should not happen if counts match, but be defensive)
		if receipt == nil {
			return errors.Errorf("nil receipt at index %d for transaction %s in block %s",
				i, tx.Hash, block.Hash,
			)
		}

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

	// Verify each trace belongs to this block.
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

func (e *EvmExtractor) Unsubscribe() error {
	if !e.running.Load() {
		return errors.New("extractor is not running")
	}

	if cancel, ok := e.cancel.Load().(context.CancelFunc); ok {
		cancel()
	}

	e.running.Store(false)
	return nil
}

func (e *EvmExtractor) Close() {
	e.Unsubscribe()
	for _, c := range e.rpcClients {
		c.Close()
	}
}
