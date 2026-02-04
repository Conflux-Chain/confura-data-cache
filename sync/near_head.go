package sync

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/process"
	"github.com/pkg/errors"
)

type NearHeadConfig struct {
	Adapter evm.AdapterConfig
	Poller  poll.Option
	Writer  nearhead.WriteOption
	Cache   nearhead.Config
}

// StartNearHead starts to sync the near head data in separate goroutines, and returns the corresponding near head cache
// to query blockchain data with high performance.
func StartNearHead(ctx context.Context, wg *sync.WaitGroup, config NearHeadConfig) (*nearhead.EthCache, error) {
	adapter, err := evm.NewAdapterWithConfig(config.Adapter)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to create EVM adapter")
	}

	// sync from the finalized block that will never be reverted
	finalizedBlockNumber, err := adapter.GetFinalizedBlockNumber(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to retrieve finalized block number")
	}

	cache := nearhead.NewEthCache(config.Cache)

	poller := poll.NewLatestPoller(adapter, finalizedBlockNumber, poll.ReorgWindowParams{}, config.Poller)
	wg.Add(1)
	go poller.Poll(ctx, wg)

	writer := nearhead.NewWriter(cache, config.Writer)
	wg.Add(1)
	go process.Process(ctx, wg, poller.DataCh(), writer)

	return cache, nil
}
