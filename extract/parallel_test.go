package extract

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/parallel"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEthParallelWorkerParallelDo(t *testing.T) {
	start := uint64(100)
	mockData := types.EthBlockData{
		Block: makeMockBlock(100, "0x100", "0x99"),
	}

	mockClient := new(MockEthRpcClient)
	mockClient.On("BlockBundleByNumber", mock.Anything, ethTypes.BlockNumber(100)).Return(mockData, nil)
	mockClient.On("BlockBundleByNumber", mock.Anything, ethTypes.BlockNumber(101)).
		Return(types.EthBlockData{}, errors.New("rpc error"))

	dataChan := NewEthMemoryBoundedChannel(math.MaxUint64)
	worker := NewEthParallelWorker(start, dataChan, mockClient)
	result, err := worker.ParallelDo(context.Background(), 0, 0)

	assert.NoError(t, err)
	assert.Equal(t, &mockData, result)

	// test rpc error
	var buf bytes.Buffer
	logrus.SetOutput(&buf)
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	result, err = worker.ParallelDo(ctx, 0, 1)
	assert.Error(t, err)
	assert.Equal(t, ctx.Err(), err)
	assert.Nil(t, result)
	assert.Contains(t, buf.String(), "rpc error")

	mockClient.AssertExpectations(t)
}

func TestParallelWorkerParallelCollect(t *testing.T) {
	mockClient := new(MockEthRpcClient)
	mockData := &types.EthBlockData{
		Block: makeMockBlock(100, "0x100", "0x99"),
	}

	dataChan := NewEthMemoryBoundedChannel(math.MaxUint64)
	worker := NewEthParallelWorker(100, dataChan, mockClient)
	err := worker.ParallelCollect(context.Background(), &parallel.Result[*types.EthBlockData]{Value: mockData})

	assert.NoError(t, err)
	assert.Equal(t, uint64(1), worker.NumCollected())
	assert.Equal(t, mockData, dataChan.Receive())

	err = worker.ParallelCollect(context.Background(), &parallel.Result[*types.EthBlockData]{Err: errors.New("rpc error")})
	assert.Error(t, err)
	assert.Equal(t, uint64(1), worker.NumCollected())
}
