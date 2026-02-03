package nearhead

import (
	"context"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/Conflux-Chain/go-conflux-util/health"
	"github.com/sirupsen/logrus"
)

type WriteOption struct {
	Health health.TimedCounterConfig
}

// Writer is used in poll-and-process model.
type Writer struct {
	cache  *EthCache
	health *health.TimedCounter
}

func NewWriter(cache *EthCache, option WriteOption) *Writer {
	return &Writer{
		cache:  cache,
		health: health.NewTimedCounter(option.Health),
	}
}

// Process implements process.Processor[poll.Revertable[evm.BlockData]] interface.
func (writer *Writer) Process(ctx context.Context, data poll.Revertable[evm.BlockData]) {
	blockNumber := data.Data.Block.Number.Uint64()

	if data.Reverted {
		popped := writer.cache.Pop(blockNumber)
		logrus.WithField("blocks", popped).Info("Near head cache popped")
	}

	convertedData := types.EthBlockData(data.Data)
	sizedData := types.NewSized(&convertedData)

	err := writer.cache.Put(&sizedData)

	writer.health.LogOnError(err, "Write near head cache")
}
