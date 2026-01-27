package sync

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	"github.com/Conflux-Chain/confura-data-cache/store"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/process"
	"github.com/pkg/errors"
)

type CatchUpConfig struct {
	Adapter evm.AdapterConfig
	Poller  poll.CatchUpOption
	Writer  store.BatchWriteOption
}

type Config struct {
	Adapter evm.AdapterConfig
	Poller  poll.Option
	Writer  store.WriteOption
}

type NearHeadConfig struct {
	Adapter evm.AdapterConfig
	Poller  poll.Option
	Writer  nearhead.WriteOption
}

// CatchUp sync data into database till the latest finalized block. Note, this is a time consuming operation, e.g. several days or weeks.
func CatchUp(ctx context.Context, config CatchUpConfig, writable store.Writable) error {
	adapter, err := evm.NewAdapterWithConfig(config.Adapter)
	if err != nil {
		return errors.WithMessage(err, "Failed to create EVM adapter")
	}

	var wg sync.WaitGroup

	poller := poll.NewCatchUpPoller(adapter, writable.NextBlockNumber(), config.Poller)
	wg.Add(1)
	go poller.Poll(ctx, &wg)

	writer := store.NewBatchWriter(writable, config.Writer)
	wg.Add(1)
	go process.Process(ctx, &wg, poller.DataCh(), writer)

	wg.Wait()

	return nil
}

// Start starts to sync data against the finalized block in a separate goroutine.
func Start(ctx context.Context, wg *sync.WaitGroup, config Config, writable store.Writable) error {
	adapter, err := evm.NewAdapterWithConfig(config.Adapter)
	if err != nil {
		return errors.WithMessage(err, "Failed to create EVM adapter")
	}

	poller := poll.NewFinalizedPoller(adapter, writable.NextBlockNumber(), config.Poller)
	wg.Add(1)
	go poller.Poll(ctx, wg)

	writer := store.NewWriter(writable, config.Writer)
	wg.Add(1)
	go process.Process(ctx, wg, poller.DataCh(), writer)

	return nil
}

// StartNearHead starts to sync the near head data in a separate goroutine.
func StartNearHead(ctx context.Context, wg *sync.WaitGroup, config NearHeadConfig, cache *nearhead.EthCache) error {
	adapter, err := evm.NewAdapterWithConfig(config.Adapter)
	if err != nil {
		return errors.WithMessage(err, "Failed to create EVM adapter")
	}

	// sync from the finalized block that will never be reverted
	finalizedBlockNumber, err := adapter.GetFinalizedBlockNumber(ctx)
	if err != nil {
		return errors.WithMessage(err, "Failed to retrieve finalized block number")
	}

	poller := poll.NewLatestPoller(adapter, finalizedBlockNumber, poll.ReorgWindowParams{}, config.Poller)
	wg.Add(1)
	go poller.Poll(ctx, wg)

	wg.Add(1)
	go process.Process(ctx, wg, poller.DataCh(), nearhead.NewWriter(cache, config.Writer))

	return nil
}
