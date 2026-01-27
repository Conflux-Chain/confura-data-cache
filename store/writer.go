package store

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/Conflux-Chain/go-conflux-util/ctxutil"
	"github.com/Conflux-Chain/go-conflux-util/health"
)

type WriteOption struct {
	RetryInterval time.Duration `default:"3s"`

	Health health.CounterConfig
}

// Writer is used in poll-and-process model.
type Writer struct {
	option WriteOption
	store  Writable
	health *health.Counter
}

func NewWriter(store Writable, option WriteOption) *Writer {
	return &Writer{
		option: option,
		store:  store,
		health: health.NewCounter(option.Health),
	}
}

// Process implements process.Processor[evm.BlockData] interface.
func (writer *Writer) Process(ctx context.Context, data evm.BlockData) {
	writer.write(ctx, types.EthBlockData(data))
}

// Process implements process.Processor[evm.BlockData] interface.
func (writer *Writer) Close(ctx context.Context) {
	// do nothing
}

func (writer *Writer) write(ctx context.Context, data ...types.EthBlockData) bool {
	for {
		err := writer.store.Write(data...)

		writer.health.LogOnError(err, "Write store")

		if err == nil {
			return true
		}

		if err = ctxutil.Sleep(ctx, writer.option.RetryInterval); err != nil {
			return false
		}
	}
}
