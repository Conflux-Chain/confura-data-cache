package sync

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/parallel"
	"github.com/ethereum/go-ethereum/common"
	"github.com/mcuadros/go-defaults"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func makeMockBlock(number uint64, hashes ...string) *ethTypes.Block {
	var blockHash, parentHash string
	if len(hashes) > 0 {
		blockHash = hashes[0]
	}
	if len(hashes) > 1 {
		parentHash = hashes[1]
	}
	return &ethTypes.Block{
		Number:     big.NewInt(int64(number)),
		Hash:       common.HexToHash(blockHash),
		ParentHash: common.HexToHash(parentHash),
	}
}

type MockExtractor struct {
	mock.Mock
}

func (m *MockExtractor) Start(ctx context.Context, ch *extract.EthMemoryBoundedChannel) {
	m.Called(ctx, ch)
}

type MockStore struct {
	mock.Mock
}

func (m *MockStore) NextBlockNumber() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockStore) Write(data ...types.EthBlockData) error {
	args := m.Called(data)
	return args.Error(0)
}

type MockCache struct {
	mock.Mock
}

func (m *MockCache) Put(data *types.Sized[*types.EthBlockData]) error {
	args := m.Called(data)
	return args.Error(0)
}

func (m *MockCache) Pop(bn uint64) bool {
	args := m.Called(bn)
	return args.Get(0).(bool)
}

func TestEthSyncerIntegration(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("TEST_EVM_RPC_ENDPOINT"))
	if len(endpoint) == 0 {
		t.Skip("no rpc endpoint provided, skip test")
		return
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	nextCacheWrite, nextStoreWrite := uint64(0), uint64(100)

	cache := new(MockCache)
	cache.On("Put", mock.Anything).Run(func(args mock.Arguments) {
		sized := args.Get(0).(*types.Sized[*types.EthBlockData])
		blockData := sized.Value
		assert.NotNil(t, blockData)
		assert.NotNil(t, blockData.Block)

		blockNum := blockData.Block.Number.Uint64()
		if nextCacheWrite > 0 {
			assert.Equal(t, nextCacheWrite, blockNum)
		}
		nextCacheWrite = blockNum + 1
	}).Return(nil)

	store := new(MockStore)
	store.On("NextBlockNumber").Return(nextStoreWrite)
	store.On("Write", mock.Anything).Run(func(args mock.Arguments) {
		data := args.Get(0).([]types.EthBlockData)
		assert.NotNil(t, data)
		assert.NotEmpty(t, data)
		assert.Len(t, data, 1)

		blockData := data[0]
		assert.NotNil(t, blockData)
		assert.NotNil(t, blockData.Block)
		assert.Equal(t, blockData.Block.Number.Uint64(), nextStoreWrite)
		nextStoreWrite = blockData.Block.Number.Uint64() + 1
	}).Return(nil)

	conf := extract.EthConfig{
		RpcEndpoint: endpoint,
		SerialOption: parallel.SerialOption{
			Routines: 1, Window: 1,
		},
	}
	defaults.SetDefaults(&conf)

	syncer, err := newEthSyncer(conf, cache, store, func(conf extract.EthConfig) (EthExtractor, error) {
		return extract.NewEthExtractor(conf)
	})
	assert.NoError(t, err)
	assert.NotNil(t, syncer)

	syncer.Run(ctx, &wg)

	wg.Wait()
	cache.AssertExpectations(t)
	store.AssertExpectations(t)
}

func TestNewEthSyncer(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		cache := new(MockCache)
		store := new(MockStore)
		store.On("NextBlockNumber").Return(uint64(100))

		syncer, err := newEthSyncer(extract.EthConfig{}, cache, store, func(extract.EthConfig) (EthExtractor, error) {
			return &MockExtractor{}, nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, syncer)
	})

	t.Run("Error", func(t *testing.T) {
		cache := new(MockCache)
		store := new(MockStore)
		store.On("NextBlockNumber").Return(uint64(100))

		syncer, err := newEthSyncer(extract.EthConfig{}, cache, store, func(conf extract.EthConfig) (EthExtractor, error) {
			return nil, errors.New("error creating extractor")
		})
		assert.Error(t, err)
		assert.Nil(t, syncer)
	})
}

func TestEthSyncerProcessNearhead(t *testing.T) {
	t.Parallel()

	t.Run("ReorgHandled", func(t *testing.T) {
		cache := new(MockCache)
		syncer := &EthSyncer{cache: cache}

		blockNumber := uint64(123)
		cache.On("Pop", blockNumber).Return(true)

		syncer.processNearhead(&extract.EthRevertableBlockData{
			ReorgHeight: &blockNumber,
		})

		cache.AssertCalled(t, "Pop", blockNumber)
	})

	t.Run("CachePutSuccess", func(t *testing.T) {
		cache := new(MockCache)

		blockData := types.EthBlockData{
			Block: makeMockBlock(123, "0x123"),
		}

		cache.On("Put", mock.MatchedBy(func(data any) bool {
			_, ok := data.(*types.Sized[*types.EthBlockData])
			return ok
		})).Return(nil)

		syncer := &EthSyncer{cache: cache}
		syncer.processNearhead(&extract.EthRevertableBlockData{BlockData: &blockData})

		cache.AssertCalled(t, "Put", mock.Anything)
	})

	t.Run("CachePutError", func(t *testing.T) {
		cache := new(MockCache)

		blockData := types.EthBlockData{
			Block: makeMockBlock(123, "0x123"),
		}

		cache.On("Put", mock.Anything).Return(errors.New("cache error")).Once()
		cache.On("Put", mock.Anything).Return(nil).Once()

		var buf bytes.Buffer
		logrus.SetOutput(&buf)

		syncer := &EthSyncer{cache: cache}
		syncer.processNearhead(&extract.EthRevertableBlockData{BlockData: &blockData})

		cache.AssertCalled(t, "Put", mock.Anything)
		assert.Contains(t, buf.String(), "cache error")
	})
}

func TestEthSyncerProcessFinalized(t *testing.T) {
	t.Parallel()

	t.Run("StoreWriteSuccess", func(t *testing.T) {
		store := new(MockStore)

		blockData := types.EthBlockData{
			Block: makeMockBlock(123, "0x123"),
		}

		store.On("Write", []types.EthBlockData{blockData}).Return(nil)

		syncer := &EthSyncer{store: store}
		syncer.processFinalized(&extract.EthRevertableBlockData{BlockData: &blockData})

		store.AssertCalled(t, "Write", mock.Anything)
	})

	t.Run("StoreWriteError", func(t *testing.T) {
		store := new(MockStore)

		blockData := types.EthBlockData{
			Block: makeMockBlock(123, "0x123"),
		}

		store.On("Write", mock.Anything).Return(errors.New("store error")).Once()
		store.On("Write", mock.Anything).Return(nil).Once()

		var buf bytes.Buffer
		logrus.SetOutput(&buf)

		syncer := &EthSyncer{store: store}
		syncer.processFinalized(&extract.EthRevertableBlockData{BlockData: &blockData})

		store.AssertCalled(t, "Write", mock.Anything)
		assert.Contains(t, buf.String(), "store error")
	})
}

func TestEthSyncerStartNearHeadSync(t *testing.T) {
	t.Parallel()

	t.Run("StartNearHeadSyncReorgHandled", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		reorgBlockNumber := uint64(100)
		reorgBlockData := &extract.EthRevertableBlockData{ReorgHeight: &reorgBlockNumber}

		cache := new(MockCache)
		cache.On("Pop", reorgBlockNumber).Return(true)

		extractor := &MockExtractor{}
		extractor.On("Start", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			ch := args.Get(1).(*extract.EthMemoryBoundedChannel)
			// Simulate sending a block
			ch.Send(types.NewSized(reorgBlockData))
		}).Return()

		syncer := &EthSyncer{
			EthConfig:         extract.EthConfig{MaxMemoryUsageBytes: 1024},
			cache:             cache,
			nearheadExtractor: extractor,
		}

		wg.Add(1)
		go syncer.startNearHeadSync(ctx, &wg)

		wg.Wait()
		assert.Error(t, ctx.Err())
		cache.AssertCalled(t, "Pop", reorgBlockNumber)
	})

	t.Run("StartNearHeadSyncCachePut", func(t *testing.T) {
		var wg sync.WaitGroup
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		blockData := &types.EthBlockData{
			Block: makeMockBlock(123, "0x123"),
		}
		revertableBlockData := &extract.EthRevertableBlockData{
			BlockData: blockData,
		}

		cache := new(MockCache)
		cache.On("Put", mock.Anything).Return(errors.New("cache error")).Once()
		cache.On("Put", mock.Anything).Return(nil).Once()

		extractor := &MockExtractor{}
		extractor.On("Start", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			ch := args.Get(1).(*extract.EthMemoryBoundedChannel)
			// Simulate sending a block
			ch.Send(types.NewSized(revertableBlockData))
		}).Return()

		var buf bytes.Buffer
		logrus.SetOutput(&buf)

		syncer := &EthSyncer{
			EthConfig:         extract.EthConfig{MaxMemoryUsageBytes: 1024},
			cache:             cache,
			nearheadExtractor: extractor,
		}

		wg.Add(1)
		go syncer.startNearHeadSync(ctx, &wg)

		wg.Wait()
		assert.Error(t, ctx.Err())
		cache.AssertCalled(t, "Put", mock.Anything)
		assert.Contains(t, buf.String(), "cache error")
	})
}

func TestEthSyncerStartFinalizedSync(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	blockData := &types.EthBlockData{
		Block: makeMockBlock(123, "0x123"),
	}
	revertableBlockData := &extract.EthRevertableBlockData{
		BlockData: blockData,
	}

	store := new(MockStore)
	store.On("Write", mock.Anything).Return(errors.New("store error")).Once()
	store.On("Write", mock.Anything).Return(nil).Once()

	extractor := &MockExtractor{}
	extractor.On("Start", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		ch := args.Get(1).(*extract.EthMemoryBoundedChannel)
		// Simulate sending a block
		ch.Send(types.NewSized(revertableBlockData))
	}).Return()

	var buf bytes.Buffer
	logrus.SetOutput(&buf)

	syncer := &EthSyncer{
		EthConfig:          extract.EthConfig{MaxMemoryUsageBytes: 1024},
		store:              store,
		finalizedExtractor: extractor,
	}

	wg.Add(1)
	go syncer.startFinalizedSync(ctx, &wg)

	wg.Wait()
	assert.Error(t, ctx.Err())
	store.AssertCalled(t, "Write", mock.Anything)
	assert.Contains(t, buf.String(), "store error")
}
