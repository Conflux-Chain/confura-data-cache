package sync

import (
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
	"github.com/Conflux-Chain/go-conflux-util/health"
	"github.com/Conflux-Chain/go-conflux-util/parallel"
	"github.com/ethereum/go-ethereum/common"
	"github.com/mcuadros/go-defaults"
	ethTypes "github.com/openweb3/web3go/types"
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

func (m *MockCache) Pop(height uint64) int {
	args := m.Called(height)
	return args.Int(0)
}

func (m *MockCache) Put(data *types.Sized[*types.EthBlockData]) error {
	args := m.Called(data)
	return args.Error(0)
}

func TestEthSyncerIntegration(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("TEST_EVM_RPC_ENDPOINT"))
	if len(endpoint) == 0 {
		t.Skip("no rpc endpoint provided, skip test")
		return
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nextStoreWrite := uint64(100)

	store := new(MockStore)
	store.On("NextBlockNumber").Return(nextStoreWrite)
	store.On("Write", mock.Anything).Run(func(args mock.Arguments) {
		data := args.Get(0).([]types.EthBlockData)
		assert.NotNil(t, data)
		assert.NotEmpty(t, data)

		blockData := data[0]
		assert.NotNil(t, blockData)
		assert.NotNil(t, blockData.Block)
		assert.Equal(t, blockData.Block.Number.Uint64(), nextStoreWrite)
		nextStoreWrite = blockData.Block.Number.Uint64() + uint64(len(data))
	}).Return(nil)

	conf := EthConfig{
		Extract: extract.EthConfig{
			RpcEndpoint: endpoint,
			SerialOption: parallel.SerialOption{
				Routines: 1, Window: 1,
			},
		},
	}
	defaults.SetDefaults(&conf)

	syncer, err := newEthSyncer(conf, store, func(conf extract.EthConfig) (EthExtractor, error) {
		return extract.NewEthExtractor(conf)
	})
	assert.NoError(t, err)
	assert.NotNil(t, syncer)

	wg.Add(1)
	go syncer.Run(ctx, &wg)

	wg.Wait()
	store.AssertExpectations(t)
}

func TestNewEthSyncer(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		store := new(MockStore)
		store.On("NextBlockNumber").Return(uint64(100))

		syncer, err := newEthSyncer(EthConfig{}, store, func(extract.EthConfig) (EthExtractor, error) {
			return &MockExtractor{}, nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, syncer)
	})

	t.Run("Error", func(t *testing.T) {
		store := new(MockStore)
		store.On("NextBlockNumber").Return(uint64(100))

		syncer, err := newEthSyncer(EthConfig{}, store, func(conf extract.EthConfig) (EthExtractor, error) {
			return nil, errors.New("error creating extractor")
		})
		assert.Error(t, err)
		assert.Nil(t, syncer)
	})
}

func TestEthSyncerProcessFinalized(t *testing.T) {
	t.Parallel()

	t.Run("StoreWriteSuccess", func(t *testing.T) {
		store := new(MockStore)

		blockData := types.EthBlockData{
			Block: makeMockBlock(123, "0x123"),
		}

		store.On("Write", mock.Anything).Return(nil)

		syncer := &EthSyncer{
			EthConfig: EthConfig{
				Persistence: PersistenceConfig{
					BatchSize: 2, BatchInterval: time.Second,
				},
			},
			store:       store,
			health:      health.NewTimedCounter(),
			lastFlushAt: time.Now(),
		}
		syncer.processFinalized(&extract.EthRevertableBlockData{BlockData: &blockData})
		store.AssertNotCalled(t, "Write", mock.Anything)

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

		syncer := &EthSyncer{
			store:  store,
			health: health.NewTimedCounter(),
		}
		syncer.processFinalized(&extract.EthRevertableBlockData{BlockData: &blockData})

		store.AssertCalled(t, "Write", mock.Anything)
	})
}

func TestEthSyncerRun(t *testing.T) {
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

	syncer := &EthSyncer{
		EthConfig: EthConfig{
			Extract: extract.EthConfig{MaxMemoryUsageBytes: 1024},
		},
		store:              store,
		finalizedExtractor: extractor,
		health:             health.NewTimedCounter(),
	}

	wg.Add(1)
	go syncer.Run(ctx, &wg)

	wg.Wait()
	assert.Error(t, ctx.Err())
	store.AssertCalled(t, "Write", mock.Anything)
}

func TestEthNearHeadSyncerIntegration(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("TEST_EVM_RPC_ENDPOINT"))
	if len(endpoint) == 0 {
		t.Skip("no rpc endpoint provided, skip test")
		return
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var latestSynchronizedBlockNum uint64
	cache := new(MockCache)

	cache.On("Put", mock.Anything).Run(func(args mock.Arguments) {
		sizedBlockData := args.Get(0).(*types.Sized[*types.EthBlockData])
		assert.NotEmpty(t, sizedBlockData)
		assert.NotNil(t, sizedBlockData.Value.Block)

		blockNum := sizedBlockData.Value.Block.Number.Uint64()
		if latestSynchronizedBlockNum > 0 {
			assert.Equal(t, blockNum, latestSynchronizedBlockNum+1)
		}
		latestSynchronizedBlockNum = blockNum
	}).Return(nil)

	cache.On("Pop", mock.Anything).Run(func(args mock.Arguments) {
		reorgHeight := args.Get(0).(uint64)
		if latestSynchronizedBlockNum > 0 {
			assert.Equal(t, latestSynchronizedBlockNum, reorgHeight)
		}
		latestSynchronizedBlockNum = reorgHeight - 1
	}).Return(true)

	conf := EthConfig{
		Extract: extract.EthConfig{
			RpcEndpoint: endpoint,
		},
	}
	defaults.SetDefaults(&conf)

	syncer, err := newEthNearHeadSyncer(conf, cache, func(conf extract.EthConfig) (EthExtractor, error) {
		return extract.NewEthExtractor(conf)
	})
	assert.NoError(t, err)
	assert.NotNil(t, syncer)

	wg.Add(1)
	go syncer.Run(ctx, &wg)

	wg.Wait()
}

func TestNewNearHeadEthSyncer(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		cache := new(MockCache)

		syncer, err := newEthNearHeadSyncer(EthConfig{}, cache, func(extract.EthConfig) (EthExtractor, error) {
			return &MockExtractor{}, nil
		})
		assert.NoError(t, err)
		assert.NotNil(t, syncer)
	})

	t.Run("Error", func(t *testing.T) {
		cache := new(MockCache)

		syncer, err := newEthNearHeadSyncer(EthConfig{}, cache, func(extract.EthConfig) (EthExtractor, error) {
			return nil, errors.New("error creating extractor")
		})
		assert.Error(t, err)
		assert.Nil(t, syncer)
	})
}

func TestEthNearHeadSyncerProcessNearHead(t *testing.T) {
	t.Parallel()

	t.Run("Pop", func(t *testing.T) {
		cache := new(MockCache)
		cache.On("Pop", uint64(123)).Return(5)

		syncer := &EthNearHeadSyncer{
			cache:  cache,
			health: health.NewTimedCounter(),
		}

		syncer.processNearHead(&extract.EthRevertableBlockData{
			ReorgHeight: ptrTo(uint64(123)),
		})

		cache.AssertCalled(t, "Pop", uint64(123))
	})

	t.Run("Put", func(t *testing.T) {
		block := &types.EthBlockData{
			Block: makeMockBlock(100, "0xabc"),
		}

		cache := new(MockCache)
		cache.On("Put", mock.Anything).Return(errors.New("temp")).Once()
		cache.On("Put", mock.Anything).Return(nil).Once()

		syncer := &EthNearHeadSyncer{
			cache:  cache,
			health: health.NewTimedCounter(),
		}

		syncer.processNearHead(&extract.EthRevertableBlockData{
			BlockData: block,
		})
		cache.AssertNumberOfCalls(t, "Put", 2)
	})
}

func TestEthNearHeadSyncerRun(t *testing.T) {
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
	cache.On("Put", mock.Anything).Return(errors.New("temp")).Once()
	cache.On("Put", mock.Anything).Return(nil).Once()

	extractor := &MockExtractor{}
	extractor.On("Start", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		ch := args.Get(1).(*extract.EthMemoryBoundedChannel)
		// Simulate sending a block
		ch.Send(types.NewSized(revertableBlockData))
	}).Return()

	syncer := &EthNearHeadSyncer{
		EthConfig: EthConfig{
			Extract: extract.EthConfig{MaxMemoryUsageBytes: 1024},
		},
		cache:             cache,
		nearHeadExtractor: extractor,
		health:            health.NewTimedCounter(),
	}

	wg.Add(1)
	go syncer.Run(ctx, &wg)

	wg.Wait()
	assert.Error(t, ctx.Err())
	cache.AssertCalled(t, "Put", mock.Anything)
}

func ptrTo[T any](v T) *T {
	return &v
}
