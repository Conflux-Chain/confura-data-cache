package extract

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/parallel"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
)

var (
	_ parallel.Interface[*types.EthBlockData] = (*EthParallelWorker)(nil)
)

type ParallelWorker[T Sizable] struct {
	start        uint64
	dataChan     *MemoryBoundedChannel[T]
	numCollected atomic.Uint64
}

func (w *ParallelWorker[T]) NumCollected() uint64 {
	return w.numCollected.Load()
}

// ParallelCollect implements parallel.Interface.
func (w *ParallelWorker[T]) ParallelCollect(ctx context.Context, result *parallel.Result[T]) error {
	if result.Err == nil {
		w.dataChan.Send(result.Value)
		w.numCollected.Add(1)
	}
	return result.Err
}

type EthParallelWorker struct {
	ParallelWorker[*types.EthBlockData]
	client EthRpcClient
}

func NewEthParallelWorker(start uint64, dataChan *EthMemoryBoundedChannel, client EthRpcClient) *EthParallelWorker {
	return &EthParallelWorker{
		ParallelWorker: ParallelWorker[*types.EthBlockData]{
			start:    start,
			dataChan: dataChan,
		},
		client: client,
	}
}

// ParallelDo implements parallel.Interface.
func (w *EthParallelWorker) ParallelDo(ctx context.Context, routine int, task int) (*types.EthBlockData, error) {
	bn := w.start + uint64(task)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		data, err := w.client.BlockBundleByNumber(ctx, ethTypes.BlockNumber(bn))
		if err == nil {
			return &data, nil
		}
		logrus.WithFields(logrus.Fields{
			"block":   bn,
			"start":   w.start,
			"task":    task,
			"routine": routine,
		}).WithError(err).Info("EthParallelWorker failed to get block bundle")
		time.Sleep(time.Second)
	}
}
