package extract

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
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
		return nil, false, errors.WithMessage(err, "failed to validate block data")
	}

	e.StartBlockNumber++
	return &blockData, false, nil
}
