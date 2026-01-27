package store

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
)

type BatchWriteOption struct {
	WriteOption `mapstructure:",squash"`

	BatchSize    int           `default:"100"`
	BatchTimeout time.Duration `default:"3s"`
}

// BatchWriter is used in poll-and-process model.
type BatchWriter struct {
	option BatchWriteOption

	inner *Writer

	buf           []types.EthBlockData
	lastBatchTime time.Time
}

func NewBatchWriter(store Writable, option BatchWriteOption) *BatchWriter {
	return &BatchWriter{
		option:        option,
		inner:         NewWriter(store, option.WriteOption),
		buf:           make([]types.EthBlockData, 0, option.BatchSize),
		lastBatchTime: time.Now(),
	}
}

// Process implements process.Processor[evm.BlockData] interface.
func (writer *BatchWriter) Process(ctx context.Context, data evm.BlockData) {
	writer.buf = append(writer.buf, types.EthBlockData(data))

	if len(writer.buf) >= writer.option.BatchSize ||
		time.Since(writer.lastBatchTime) >= writer.option.BatchTimeout {
		writer.write(ctx)
	}
}

// Process implements process.Processor[evm.BlockData] interface.
func (writer *BatchWriter) Close(ctx context.Context) {
	if len(writer.buf) > 0 {
		writer.write(ctx)
	}
}

func (writer *BatchWriter) write(ctx context.Context) {
	if writer.inner.write(ctx, writer.buf...) {
		writer.buf = writer.buf[:0]
		writer.lastBatchTime = time.Now()
	}
}
