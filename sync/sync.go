package sync

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/openweb3/go-rpc-provider"
	"github.com/openweb3/web3go"
	"github.com/pkg/errors"
)

type Config struct {
	// Number of blocks for each batch sync
	BatchSize uint64 `default:"10"`
	// The head tag representing the block number to which the sync anchors
	// The default value is "latest", which means the latest block number.
	// Other values can be "earliest", "pending" etc.
	HeadTag string `default:"latest"`
	// Interval to sync data in normal mode
	NormalInterval time.Duration `default:"1s"`
	// Interval to sync data for catch-up mode
	CatchUpInterval time.Duration `default:"1ms"`
	// Buffered channel size to store the queried block data
	ResultChannelSize int `default:"200"`
}

// EvmSyncer is an EVM compatible blockchain data syncer.
// TODO:
// 1. add pivot reorg check to ensure data consistency
// 2. use round robin to select the rpc client if the current client is not available for high availability
// 3. add catch-up sync to allow concurrent sync of multiple blocks
// 4. coordinate chain team to provide an API to query traces by block hash to ensure data consistency
// 5. add metrics, logging and monitoring
// 6. add graceful shutdown to ensure data consistency
type EvmSyncer struct {
	Config

	// The storage to store the synced data
	store *leveldb.Store
	// Running status
	status atomic.Bool
	// RPC clients
	clients []*web3go.Client
	// Selected RPC client index
	clientIdx atomic.Uint32
	// The latest block number to sync by
	headBlockNumber rpc.BlockNumber
}

func NewEvmSyncer(store *leveldb.Store, clients []*web3go.Client, conf Config) (*EvmSyncer, error) {
	if len(clients) == 0 {
		return nil, errors.New("no rpc clients provided")
	}

	var headBlockNumber rpc.BlockNumber
	if err := json.Unmarshal([]byte(`"`+conf.HeadTag+`"`), &headBlockNumber); err != nil {
		return nil, errors.New("invalid head tag")
	}

	syncer := &EvmSyncer{
		Config:          conf,
		store:           store,
		clients:         clients,
		headBlockNumber: headBlockNumber,
	}
	return syncer, nil
}

// Sync starts the sync process and returns a channel to receive the synced block data.
// The sync process will run in a separate goroutine. If the sync process is already started,
// an error will be returned. The sync process will run until the context is done.
func (syncer *EvmSyncer) Sync(ctx context.Context) (<-chan types.EthBlockData, error) {
	if !syncer.status.CompareAndSwap(false, true) {
		return nil, errors.New("sync already started")
	}
	defer syncer.status.Store(false)

	resultChan := make(chan types.EthBlockData, syncer.ResultChannelSize)
	defer close(resultChan)

	go syncer.run(ctx, resultChan)
	return resultChan, nil
}

func (syncer *EvmSyncer) run(ctx context.Context, resultChan chan<- types.EthBlockData) {
	ticker := time.NewTimer(syncer.CatchUpInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			result, aligned, err := syncer.syncOnce(ctx)
			if err == nil {
				for _, result := range result {
					resultChan <- result
				}
			}
			if err != nil || aligned {
				ticker.Reset(syncer.NormalInterval)
			} else {
				ticker.Reset(syncer.CatchUpInterval)
			}
		}
	}
}

func (syncer *EvmSyncer) syncOnce(ctx context.Context) ([]types.EthBlockData, bool, error) {
	client := syncer.clients[syncer.clientIdx.Load()]
	nextBlockNumber := syncer.store.NextBlockNumber()

	// Get the latest block number
	latestBlock, err := client.Eth.BlockByNumber(syncer.headBlockNumber, false)
	if err != nil {
		return nil, false, errors.WithMessagef(err, "failed to get the block with tag `%v`", syncer.headBlockNumber)
	}
	if latestBlock == nil {
		return nil, false, errors.Errorf("invalid block with tag `%v`", syncer.HeadTag)
	}

	// If the latest block number is less than the next block number, it means the syncer is already catched up.
	// In this case, we can return a flag indicating that the syncer is aligned. Otherwise, we can return
	// the block data for the next batch of blocks.
	latestBlockNumber := latestBlock.Number.Uint64()
	if nextBlockNumber > latestBlockNumber {
		return nil, true, nil
	}

	var result []types.EthBlockData
	toBlockNumber := min(nextBlockNumber+syncer.BatchSize-1, latestBlockNumber)
	for bn := nextBlockNumber; bn <= toBlockNumber; bn++ {
		blockData, err := QueryEthData(ctx, client, bn)
		if err != nil {
			return nil, false, errors.WithMessagef(err, "failed to query block data for number %v", bn)
		}
		result = append(result, *blockData)
	}
	return result, false, nil
}
