package nearhead

import (
	"context"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/poll"
	"github.com/sirupsen/logrus"
)

type Writer struct {
	cache *EthCache
}

func NewWriter(cache *EthCache) *Writer {
	return &Writer{
		cache: cache,
	}
}

// Process implements process.Processor[poll.evm.BlockData[evm.BlockData]] interface.
func (writer *Writer) Process(ctx context.Context, data poll.Revertable[evm.BlockData]) {
	if data.Reverted {
		blockNumber := data.Data.Block.Number.Uint64()
		popped := writer.cache.Pop(blockNumber)
		logrus.WithField("popped", popped).Info("Pop blockchain data from near head cache")
	}

	convertedData := types.EthBlockData(data.Data)
	sizedData := types.NewSized(&convertedData)

	if err := writer.cache.Put(&sizedData); err != nil {
		// poll-process model should guarantee the block number sequence
		logrus.WithError(err).Fatal("Failed to write near head cache")
	}
}

// Process implements process.Processor[evm.BlockData] interface.
func (writer *Writer) Close(ctx context.Context) {
	// do nothing
}
