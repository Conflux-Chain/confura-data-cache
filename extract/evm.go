package extract

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/parallel"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	_ EthRpcClient = (*Web3ClientAdapter)(nil)
)

// EthRpcClient defines a client interface for accessing Ethereum block data.
type EthRpcClient interface {
	io.Closer

	// BlockHeaderByNumber fetches only the block header for a given block number (no full tx data).
	BlockHeaderByNumber(ctx context.Context, bn ethTypes.BlockNumber) (*ethTypes.Block, error)

	// BlockBundleByNumber fetches full block data including transactions, receipts, logs, traces, etc.
	BlockBundleByNumber(ctx context.Context, bn ethTypes.BlockNumber) (types.EthBlockData, error)
}

// Web3ClientAdapter implements `EthRpcClient` by adapting a web3go client.
type Web3ClientAdapter struct {
	client *web3go.Client
}

// Close releases any held resources, such as network connections.
func (p *Web3ClientAdapter) Close() error {
	p.client.Close()
	return nil
}

// BlockHeaderByNumber retrieves a light version of the block (header + tx hashes only).
func (p *Web3ClientAdapter) BlockHeaderByNumber(ctx context.Context, bn ethTypes.BlockNumber) (*ethTypes.Block, error) {
	return p.client.WithContext(ctx).Eth.BlockByNumber(bn, false)
}

// BlockBundleByNumber retrieves a full block bundle including associated data.
func (p *Web3ClientAdapter) BlockBundleByNumber(ctx context.Context, bn ethTypes.BlockNumber) (types.EthBlockData, error) {
	startAt := time.Now()
	data, err := types.QueryEthBlockData(p.client.WithContext(ctx), uint64(bn))

	ethMetrics.Latency(err == nil).Update(time.Since(startAt).Nanoseconds())
	ethMetrics.Availability().Mark(err == nil)
	return data, err
}

func newEthFinalizedHeightProvider(c EthRpcClient) FinalizedHeightProvider {
	return func() (uint64, error) {
		block, err := c.BlockHeaderByNumber(context.Background(), ethTypes.FinalizedBlockNumber)
		if err != nil || block == nil {
			return 0, err
		}
		return block.Number.Uint64(), nil
	}
}

// EthExtractor is an EVM compatible blockchain data extractor.
type EthExtractor struct {
	EthConfig

	hashCache *BlockHashCache // Cache of recent block hashes for detecting reorgs
	rpcClient EthRpcClient    // Connected RPC client for fetching blockchain data
}

func NewEthExtractor(conf EthConfig, provider ...FinalizedHeightProvider) (*EthExtractor, error) {
	if len(conf.RpcEndpoint) == 0 {
		return nil, errors.New("no rpc endpoint provided")
	}

	client, err := web3go.NewClient(conf.RpcEndpoint)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to create rpc client for endpoint %s", conf.RpcEndpoint)
	}

	rpcClient := &Web3ClientAdapter{client: client}
	return newEthExtractorWithClient(rpcClient, conf, provider...)
}

func newEthExtractorWithClient(rpcClient EthRpcClient, conf EthConfig, provider ...FinalizedHeightProvider) (*EthExtractor, error) {
	// Normalize start block number if necessary
	if conf.StartBlockNumber < 0 {
		block, err := rpcClient.BlockHeaderByNumber(context.Background(), conf.StartBlockNumber)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to normalize start block")
		}
		conf.StartBlockNumber = ethTypes.BlockNumber(block.Number.Int64())
	}

	var finalizeProvider FinalizedHeightProvider
	if len(provider) > 0 {
		finalizeProvider = provider[0]
	} else {
		finalizeProvider = newEthFinalizedHeightProvider(rpcClient)
	}

	extractor := &EthExtractor{
		EthConfig: conf,
		rpcClient: rpcClient,
		hashCache: NewBlockHashCacheWithProvider(finalizeProvider),
	}
	return extractor, nil
}

// AppendBlockHash appends block number and hash into cache (must be in order).
// This ensures that a known-good hash exists for future reorg detection.
func (e *EthExtractor) AppendBlockHash(blockNumber uint64, blockHash common.Hash) error {
	return e.hashCache.Append(blockNumber, blockHash)
}

// Start starts the data extraction process. It will block until the context is canceled.
// It is the caller's responsibility to cancel the context when the extractor is no longer needed.
// The dataChan is used to send the extracted data to the consumer, which should be buffered to avoid
// blocking the extractor.
func (e *EthExtractor) Start(ctx context.Context, dataChan *EthMemoryBoundedChannel) {
	defer e.rpcClient.Close()

	// Catch up to the latest finalized block.
	e.catchUpUntilFinalized(ctx, dataChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		resultData, caughtUp, err := e.extractOnce(ctx)
		if resultData != nil {
			if dataChan.Send(types.NewSized(resultData)) != nil { // channel closed?
				return
			}
		}

		if err != nil || caughtUp {
			time.Sleep(e.PollInterval)
		}
	}
}

// catchUpUntilFinalized catches up to the latest finalized block.
func (e *EthExtractor) catchUpUntilFinalized(ctx context.Context, dataChan *EthMemoryBoundedChannel) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err, done := e.catchUpOnce(ctx, dataChan)
		if err != nil {
			logrus.WithError(err).Error("failed to catch up to finalized block")
			time.Sleep(e.PollInterval)
			continue
		}
		if done {
			e.hashCache.Flush()
			return
		}
	}
}

func (e *EthExtractor) catchUpOnce(ctx context.Context, dataChan *EthMemoryBoundedChannel) (error, bool) {
	latestFinalizedBlock, err := e.rpcClient.BlockHeaderByNumber(ctx, ethTypes.FinalizedBlockNumber)
	if err != nil {
		return errors.WithMessage(err, "failed to get finalized block"), false
	}

	finalizedBlockNumber := ethTypes.BlockNumber(latestFinalizedBlock.Number.Int64())
	if e.StartBlockNumber > finalizedBlockNumber {
		return nil, true
	}

	worker := NewEthParallelWorker(uint64(e.StartBlockNumber), dataChan, e.rpcClient)
	numTasks := finalizedBlockNumber - e.StartBlockNumber + 1

	err = parallel.Serial(ctx, worker, int(numTasks), e.SerialOption)
	e.StartBlockNumber += ethTypes.BlockNumber(worker.NumCollected())
	if err != nil {
		return err, false
	}

	return nil, false
}

// extractOnce fetches the block data for the current block number.
func (e *EthExtractor) extractOnce(ctx context.Context) (*EthRevertableBlockData, bool, error) {
	startAt := time.Now()

	// Get the alignment block header to determine the actual target block number to synchronize against.
	targetBlock, err := e.rpcClient.BlockHeaderByNumber(ctx, e.TargetBlockNumber)
	if err != nil {
		return nil, false, errors.WithMessage(err, "failed to get target block")
	}
	if targetBlock == nil { // Should not happen but defensive check.
		return nil, false, errors.Errorf("target block %v not found", e.TargetBlockNumber)
	}

	// Check if we are already caught up.
	if e.StartBlockNumber > ethTypes.BlockNumber(targetBlock.Number.Int64()) {
		return nil, true, nil
	}

	// Fetch the block data for the current block number.
	blockData, err := e.rpcClient.BlockBundleByNumber(ctx, ethTypes.BlockNumber(e.StartBlockNumber))
	if err != nil {
		return nil, false, errors.WithMessage(err, "failed to fetch block data")
	}

	// Verify the block data
	if err := blockData.Verify(); err != nil {
		return nil, false, NewInconsistentChainDataError(err.Error())
	}

	// Check for reorgs by comparing the block parent hash with the hashes in the hash window.
	if bn, bh, ok := e.hashCache.Latest(); ok {
		if bh != blockData.Block.ParentHash { // reorg detected
			e.StartBlockNumber = ethTypes.BlockNumber(bn)
			e.hashCache.Pop()

			detail := fmt.Sprintf(
				"reorg detected: expected parent hash %s, got %s", bh, blockData.Block.ParentHash,
			)
			return NewEthRevertableBlockDataWithReorg(bn), false, NewInconsistentChainDataError(detail)
		}
	} else {
		// TODO: Support custom reorg check function from users if block hash is missing.
		logrus.WithFields(logrus.Fields{
			"blockNumber": blockData.Block.Number,
			"blockHash":   blockData.Block.Hash,
			"parentHash":  blockData.Block.ParentHash,
		}).Info("Reorg check skipped: block hash not found in cache")
	}

	// Append the block hash into the cache for future reorg checks.
	bn, bh := blockData.Block.Number.Uint64(), blockData.Block.Hash
	if err := e.hashCache.Append(bn, bh); err != nil {
		return nil, false, errors.WithMessage(err, "failed to cache block hash")
	}

	e.StartBlockNumber++
	ethMetrics.Qps().UpdateSince(startAt)

	return NewEthRevertableBlockData(&blockData), false, nil
}
