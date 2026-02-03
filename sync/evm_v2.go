package sync

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	"github.com/Conflux-Chain/confura-data-cache/store"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/process"
	"github.com/Conflux-Chain/go-conflux-util/ctxutil"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type CatchUpConfig struct {
	Adapter evm.AdapterConfig
	Poller  poll.CatchUpOption
	Writer  store.BatchWriteOption
}

type Config struct {
	CatchUp CatchUpConfig

	Adapter evm.AdapterConfig
	Poller  poll.Option
	Writer  store.WriteOption
}

type NearHeadConfig struct {
	Adapter evm.AdapterConfig
	Poller  poll.Option
	Writer  nearhead.WriteOption
}

type Worker struct {
	config         Config
	store          store.Writable
	catchUpAdapter *evm.Adapter
	adapter        *evm.Adapter
}

func NewWorker(config Config, store store.Writable) (*Worker, error) {
	worker := &Worker{
		config: config,
		store:  store,
	}

	var err error

	if worker.adapter, err = evm.NewAdapterWithConfig(config.Adapter); err != nil {
		return nil, errors.WithMessage(err, "Failed to create EVM adapter for normal sync")
	}

	if len(config.CatchUp.Adapter.URL) > 0 {
		if worker.catchUpAdapter, err = evm.NewAdapterWithConfig(config.CatchUp.Adapter); err != nil {
			return nil, errors.WithMessage(err, "Failed to create catch-up EVM adapter")
		}
	}

	return worker, nil
}

func (worker *Worker) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// catch up to the latest finalized at first
	worker.catchUp(ctx)

	// terminate data sync if interrupted
	if ctxutil.IsDone(ctx) {
		return
	}

	// continue to sync against the latest finalized block
	nextBlockNumber := worker.store.NextBlockNumber()
	logrus.WithField("next", nextBlockNumber).Info("Start to sync data")

	poller := poll.NewFinalizedPoller(worker.adapter, nextBlockNumber, worker.config.Poller)
	wg.Add(1)
	go poller.Poll(ctx, wg)

	writer := store.NewWriter(worker.store, worker.config.Writer)
	wg.Add(1)
	go process.Process(ctx, wg, poller.DataCh(), writer)
}

func (worker *Worker) catchUp(ctx context.Context) {
	if worker.catchUpAdapter == nil {
		logrus.Info("Skip catch up mode due to node URL not specified")
		return
	}

	nextBlockNumber := worker.store.NextBlockNumber()
	logrus.WithField("next", nextBlockNumber).Info("Start to sync data in catch up mode")

	var wg sync.WaitGroup

	poller := poll.NewCatchUpPoller(worker.catchUpAdapter, nextBlockNumber, worker.config.CatchUp.Poller)
	wg.Add(1)
	go poller.Poll(ctx, &wg)

	writer := store.NewBatchWriter(worker.store, worker.config.CatchUp.Writer)
	wg.Add(1)
	go process.ProcessCatchUp(ctx, &wg, poller.DataCh(), writer)

	wg.Wait()

	logrus.WithField("next", poller.NextBlockNumber()).Info("Complete to sync data in catch up mode")
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

	writer := nearhead.NewWriter(cache, config.Writer)
	wg.Add(1)
	go process.Process(ctx, wg, poller.DataCh(), writer)

	return nil
}
