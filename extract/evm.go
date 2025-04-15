package extract

import (
	"context"
	"fmt"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
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

	hashCache *BlockHashCache // Cache of recent block hashes for detecting reorgs
	rpcClient *web3go.Client  // Connected RPC client for fetching blockchain data
}

func NewEvmExtractor(conf EthConfig) (*EthExtractor, error) {
	if len(conf.RpcEndpoint) == 0 {
		return nil, errors.New("no rpc endpoint provided")
	}

	client, err := web3go.NewClient(conf.RpcEndpoint)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to create rpc client for endpoint %s", conf.RpcEndpoint)
	}

	finalizedBlockProvider := func() (uint64, error) {
		block, err := client.Eth.BlockByNumber(rpc.FinalizedBlockNumber, false)
		if err != nil {
			return 0, err
		}
		return block.Number.Uint64(), nil
	}

	return &EthExtractor{
		EthConfig: conf,
		rpcClient: client,
		hashCache: NewBlockHashCache(0, finalizedBlockProvider),
	}, nil
}

// Start starts the data extraction process. It will block until the context is canceled.
// It is the caller's responsibility to cancel the context when the extractor is no longer needed.
// The dataChan is used to send the extracted data to the consumer, which should be buffered to avoid
// blocking the extractor.
func (e *EthExtractor) Start(ctx context.Context, dataChan chan<- types.EthBlockData) {
	defer e.rpcClient.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		blockData, caughtUp, err := e.extractOnce()
		if err == nil {
			dataChan <- *blockData
		}

		if err != nil || caughtUp {
			time.Sleep(e.PollInterval)
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

	// Verify the block data
	if err := blockData.Verify(); err != nil {
		return nil, false, NewInconsistentChainDataError(err.Error())
	}

	// Check for reorgs by comparing the block parent hash with the hashes in the hash window.
	if bn, bh, ok := e.hashCache.Latest(); ok && bh != blockData.Block.ParentHash {
		e.StartBlockNumber = bn
		e.hashCache.Pop()

		detail := fmt.Sprintf(
			"reorg detected: expected parent hash %s, got %s", bh, blockData.Block.ParentHash,
		)
		return nil, false, NewInconsistentChainDataError(detail)
	} else {
		// TODO: Support custom reorg check function from users if block hash is missing.
		logrus.WithFields(logrus.Fields{
			"blockNumber": blockData.Block.Number,
			"blockHash":   blockData.Block.Hash,
			"parentHash":  blockData.Block.ParentHash,
		}).Warn("Reorg check skipped: block hash not found in cache")
	}

	// Append the block hash into the cache for future reorg checks.
	bn, bh := blockData.Block.Number.Uint64(), blockData.Block.Hash
	if err := e.hashCache.Append(bn, bh); err != nil {
		return nil, false, errors.WithMessage(err, "failed to cache block hash")
	}

	e.StartBlockNumber++
	return &blockData, false, nil
}
