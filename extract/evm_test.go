package extract

import (
	"context"
	"math"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/parallel"
	"github.com/ethereum/go-ethereum/common"
	"github.com/mcuadros/go-defaults"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockEthRpcClient struct {
	mock.Mock
}

func (m *MockEthRpcClient) BlockHeaderByNumber(ctx context.Context, bn ethTypes.BlockNumber) (*ethTypes.Block, error) {
	args := m.Called(ctx, bn)
	return args.Get(0).(*ethTypes.Block), args.Error(1)
}

func (m *MockEthRpcClient) BlockBundleByNumber(ctx context.Context, bn ethTypes.BlockNumber) (types.EthBlockData, error) {
	args := m.Called(ctx, bn)
	return args.Get(0).(types.EthBlockData), args.Error(1)
}

func (m *MockEthRpcClient) Close() error {
	return m.Called().Error(0)
}

func makeMockBlock(number uint64, hash, parent string) *ethTypes.Block {
	return &ethTypes.Block{
		Number:     big.NewInt(int64(number)),
		Hash:       common.HexToHash(hash),
		ParentHash: common.HexToHash(parent),
	}
}

func newMockExtractor(cfg EthConfig, c *MockEthRpcClient, cache *BlockHashCache) *EthExtractor {
	return &EthExtractor{
		EthConfig: cfg,
		rpcClient: c,
		hashCache: cache,
	}
}

func TestEvmExtractIntegration(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("TEST_EVM_RPC_ENDPOINT"))
	if len(endpoint) == 0 {
		t.Skip("no rpc endpoint provided, skip test")
		return
	}

	conf := EthConfig{RpcEndpoint: endpoint}
	defaults.SetDefaults(&conf)

	extractor, err := NewEthExtractor(conf)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataChan := NewEthMemoryBoundedChannel(math.MaxInt)
	go extractor.Start(ctx, dataChan)

	data, err := dataChan.Receive()
	assert.NoError(t, err)
	assert.NotNil(t, data)
	assert.Nil(t, data.ReorgHeight)
	assert.NotNil(t, data.BlockData)
	assert.NotNil(t, data.BlockData.Block)
	assert.NotNil(t, data.BlockData.Receipts)
	assert.NotNil(t, data.BlockData.Traces)
}

func TestNewEvmExtractor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      EthConfig
		expectError bool
		errorText   string
	}{
		{
			"NoRpcEndpoint", EthConfig{
				StartBlockNumber: ethTypes.BlockNumber(100),
			}, true, "no rpc endpoint provided",
		},
		{
			"InvalidRpcEndpoint", EthConfig{
				StartBlockNumber: ethTypes.BlockNumber(100),
				RpcEndpoint:      "invalid",
			}, true, "failed to create rpc client"},
		{
			"ValidConfig", EthConfig{
				StartBlockNumber: ethTypes.BlockNumber(100),
				RpcEndpoint:      "http://localhost:8545",
			}, false, "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ext, err := NewEthExtractor(tc.config)
			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorText)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ext)
			}
		})
	}

	t.Run("CustomFinalizedProviderError", func(t *testing.T) {
		cfg := EthConfig{
			StartBlockNumber:  ethTypes.BlockNumber(100),
			TargetBlockNumber: ethTypes.FinalizedBlockNumber,
			RpcEndpoint:       "http://localhost:8545",
		}
		c := new(MockEthRpcClient)
		c.On("BlockHeaderByNumber", mock.Anything, cfg.TargetBlockNumber).Return((*ethTypes.Block)(nil), errors.New("rpc error"))
		ex, err := NewEthExtractor(cfg, newEthFinalizedHeightProvider(c))
		assert.NoError(t, err)

		err = ex.hashCache.Append(1, common.HexToHash("0x1"))
		assert.ErrorContains(t, err, "failed to get finalized block number")
	})

	t.Run("CustomFinalizedProviderOk", func(t *testing.T) {
		cfg := EthConfig{
			TargetBlockNumber: ethTypes.FinalizedBlockNumber,
			RpcEndpoint:       "http://localhost:8545",
			StartBlockNumber:  ethTypes.BlockNumber(100),
		}
		c := new(MockEthRpcClient)
		c.On("BlockHeaderByNumber", mock.Anything, cfg.TargetBlockNumber).Return(makeMockBlock(100, "0x100", "0x99"), nil)
		ex, err := NewEthExtractor(cfg, newEthFinalizedHeightProvider(c))
		assert.NoError(t, err)

		err = ex.hashCache.Append(1, common.HexToHash("0x1"))
		assert.NoError(t, err)

		bn, bh, ok := ex.hashCache.Latest()
		assert.True(t, ok)
		assert.Equal(t, uint64(1), bn)
		assert.Equal(t, common.HexToHash("0x1"), bh)
	})

	t.Run("NormalizedStartBlockNumberOK", func(t *testing.T) {
		cfg := EthConfig{
			StartBlockNumber:  ethTypes.FinalizedBlockNumber,
			TargetBlockNumber: ethTypes.LatestBlockNumber,
		}
		c := new(MockEthRpcClient)
		c.On("BlockHeaderByNumber", mock.Anything, cfg.StartBlockNumber).Return(makeMockBlock(100, "0x100", "0x99"), nil)

		ex, err := newEthExtractorWithClient(c, cfg)
		assert.NoError(t, err)
		assert.NotEqual(t, ethTypes.FinalizedBlockNumber, ex.StartBlockNumber)
		assert.Equal(t, ethTypes.BlockNumber(100), ex.StartBlockNumber)
	})

	t.Run("NormalizedStartBlockNumberError", func(t *testing.T) {
		cfg := EthConfig{
			StartBlockNumber:  ethTypes.FinalizedBlockNumber,
			TargetBlockNumber: ethTypes.LatestBlockNumber,
		}
		c := new(MockEthRpcClient)
		c.On("BlockHeaderByNumber", mock.Anything, cfg.StartBlockNumber).Return((*ethTypes.Block)(nil), errors.New("rpc error"))

		ex, err := newEthExtractorWithClient(c, cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to normalize start block")
		assert.Nil(t, ex)
	})
}

func TestEthExtractorExtractOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	block := makeMockBlock(100, "0x100", "0x99")

	tests := []struct {
		name        string
		setup       func(*MockEthRpcClient)
		cfg         EthConfig
		cache       *BlockHashCache
		wantErr     string
		caught      bool
		resultCheck func(result *EthRevertableBlockData)
	}{
		{
			"TargetBlockError",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return((*ethTypes.Block)(nil), errors.New("rpc error"))
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber},
			nil,
			"failed to get target block",
			false,
			nil,
		},
		{
			"NilTargetBlock",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return((*ethTypes.Block)(nil), nil)
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber},
			nil,
			"not found",
			false,
			nil,
		},
		{
			"CaughtUp",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber, StartBlockNumber: 101},
			nil,
			"",
			true,
			nil,
		},
		{
			"BlockBundleError",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
				c.On("BlockBundleByNumber", ctx, ethTypes.BlockNumber(99)).Return(types.EthBlockData{}, errors.New("rpc error"))
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber, StartBlockNumber: 99},
			nil,
			"failed to fetch block data",
			false,
			nil,
		},
		{
			"VerifyError",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
				blockData := types.EthBlockData{
					Block:    makeMockBlock(99, "0x99", "0x98"),
					Receipts: []ethTypes.Receipt{{TransactionHash: common.HexToHash("0xaaab")}},
				}
				blockData.Block.Transactions = *ethTypes.NewTxOrHashListByHashes([]common.Hash{common.HexToHash("0xaaaa")})
				c.On("BlockBundleByNumber", ctx, ethTypes.BlockNumber(99)).Return(blockData, nil)
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber, StartBlockNumber: 99},
			nil,
			"inconsistent chain data",
			false,
			nil,
		},
		{
			"ReorgDetected",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
				blockData := types.EthBlockData{Block: makeMockBlock(99, "0x99", "0x98")}
				c.On("BlockBundleByNumber", ctx, ethTypes.BlockNumber(99)).Return(blockData, nil)
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber, StartBlockNumber: 99},
			func() *BlockHashCache {
				h := NewBlockHashCache(0)
				h.Append(98, common.HexToHash("0x98e"))
				return h
			}(),
			"reorg detected",
			false,
			func(result *EthRevertableBlockData) {
				assert.NotNil(t, result)
				assert.NotNil(t, result.ReorgHeight)
				assert.Nil(t, result.BlockData)
				assert.Equal(t, uint64(98), *result.ReorgHeight)
			},
		},
		{
			"ReorgCheckSkipped",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
				c.On("BlockBundleByNumber", ctx, ethTypes.BlockNumber(99)).Return(types.EthBlockData{
					Block: makeMockBlock(99, "0x99", "0x98"),
				}, nil)
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber, StartBlockNumber: 99},
			NewBlockHashCache(0),
			"",
			false,
			nil,
		},
		{
			"HashCacheAppendError",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
				c.On("BlockBundleByNumber", ctx, ethTypes.BlockNumber(99)).Return(types.EthBlockData{
					Block: makeMockBlock(99, "0x99", "0x98"),
				}, nil)
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber, StartBlockNumber: 99},
			func() *BlockHashCache {
				h := NewBlockHashCache(0)
				h.Append(97, common.HexToHash("0x98"))
				return h
			}(),
			"block number not continuous",
			false,
			nil,
		},
		{
			"Ok",
			func(c *MockEthRpcClient) {
				c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
				c.On("BlockBundleByNumber", ctx, ethTypes.BlockNumber(99)).Return(types.EthBlockData{
					Block: makeMockBlock(99, "0x99", "0x98"),
				}, nil)
			},
			EthConfig{TargetBlockNumber: ethTypes.LatestBlockNumber, StartBlockNumber: 99},
			func() *BlockHashCache {
				h := NewBlockHashCache(0)
				h.Append(98, common.HexToHash("0x98"))
				return h
			}(),
			"",
			false,
			func(result *EthRevertableBlockData) {
				assert.NotNil(t, result)
				assert.Nil(t, result.ReorgHeight)
				assert.NotNil(t, result.BlockData)
				assert.Equal(t, uint64(99), result.BlockData.Block.Number.Uint64())
				assert.Equal(t, common.HexToHash("0x99"), result.BlockData.Block.Hash)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := new(MockEthRpcClient)
			tc.setup(c)
			ex := newMockExtractor(tc.cfg, c, tc.cache)
			result, caughtUp, err := ex.extractOnce(ctx)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.caught, caughtUp)

			if tc.resultCheck != nil {
				tc.resultCheck(result)
			}
		})
	}
}

func TestEthExtractorStart(t *testing.T) {
	t.Run("ContextCancellation", func(t *testing.T) {
		c := new(MockEthRpcClient)
		c.On("Close").Return(nil)
		c.On("BlockHeaderByNumber", mock.Anything, ethTypes.FinalizedBlockNumber).
			Return(makeMockBlock(100, "0x100", "0x99"), nil)

		ctx, cancel := context.WithCancel(context.Background())
		ex := newMockExtractor(EthConfig{PollInterval: time.Millisecond, StartBlockNumber: 101}, c, nil)

		dataChan := NewEthMemoryBoundedChannel(math.MaxInt)

		go ex.Start(ctx, dataChan)
		cancel()

		time.Sleep(10 * time.Millisecond)
		c.AssertCalled(t, "Close")
	})

	t.Run("SuccessfulExtraction", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		c := new(MockEthRpcClient)
		c.On("Close").Return(nil)
		block := makeMockBlock(100, "0x100", "0x99")
		c.On("BlockHeaderByNumber", ctx, ethTypes.LatestBlockNumber).Return(block, nil)
		c.On("BlockBundleByNumber", ctx, ethTypes.BlockNumber(100)).Return(types.EthBlockData{Block: block}, nil)
		c.On("BlockHeaderByNumber", ctx, ethTypes.FinalizedBlockNumber).Return(makeMockBlock(99, "0x99", "0x98"), nil)

		cache := NewBlockHashCache(0)
		cache.Append(99, common.HexToHash("0x99"))

		ex := newMockExtractor(EthConfig{
			TargetBlockNumber: ethTypes.LatestBlockNumber,
			StartBlockNumber:  100,
			PollInterval:      time.Millisecond,
		}, c, cache)

		dataChan := NewEthMemoryBoundedChannel(math.MaxInt)
		go ex.Start(ctx, dataChan)

		resultChan := make(chan *EthRevertableBlockData)
		go func() {
			data, err := dataChan.Receive()
			assert.NoError(t, err)
			resultChan <- data
		}()

		select {
		case data := <-resultChan:
			assert.NotNil(t, data)
			assert.Nil(t, data.ReorgHeight)
			assert.NotNil(t, data.BlockData)
			assert.Equal(t, uint64(100), data.BlockData.Block.Number.Uint64())
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timed out waiting for block data")
		}
	})
}

func TestEthExtractorCatchUpUntilFinalized(t *testing.T) {
	c := new(MockEthRpcClient)
	for i := 98; i <= 100; i++ {
		blockData := types.EthBlockData{
			Block: &ethTypes.Block{Number: big.NewInt(int64(i))},
		}
		c.On("BlockBundleByNumber", mock.Anything, ethTypes.BlockNumber(i)).Return(blockData, nil)
	}
	c.On("BlockHeaderByNumber", mock.Anything, ethTypes.FinalizedBlockNumber).
		Return(makeMockBlock(100, "0x100", "0x99"), nil)

	conf := EthConfig{SerialOption: parallel.SerialOption{Routines: 2}, StartBlockNumber: 98}
	ex := newMockExtractor(conf, c, NewBlockHashCache(0))
	dataChan := NewEthMemoryBoundedChannel(math.MaxInt)
	ex.catchUpUntilFinalized(context.Background(), dataChan)

	for i := 98; i <= 100; i++ {
		resultChan := make(chan *EthRevertableBlockData)
		go func() {
			data, err := dataChan.Receive()
			assert.NoError(t, err)
			resultChan <- data
		}()

		select {
		case data := <-resultChan:
			assert.NotNil(t, data)
			assert.Nil(t, data.ReorgHeight)
			assert.NotNil(t, data.BlockData)
			assert.Equal(t, uint64(i), data.BlockData.Block.Number.Uint64())
		case <-time.After(100 * time.Millisecond):
			close(resultChan)
			t.Fatal("Timed out waiting for block data")
		}
	}
	assert.Equal(t, ex.StartBlockNumber, ethTypes.BlockNumber(101))
}

func TestCatchUpOnce(t *testing.T) {
	t.Run("ErrorGettingFinalizedBlock", func(t *testing.T) {
		c := new(MockEthRpcClient)
		c.On("BlockHeaderByNumber", mock.Anything, ethTypes.FinalizedBlockNumber).
			Return((*ethTypes.Block)(nil), errors.New("rpc error"))

		conf := EthConfig{SerialOption: parallel.SerialOption{Routines: 2}, StartBlockNumber: 98}
		ex := newMockExtractor(conf, c, nil)

		dataChan := NewEthMemoryBoundedChannel(math.MaxInt)
		err, done := ex.catchUpOnce(context.Background(), dataChan)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rpc error")
		assert.False(t, done)
	})

	t.Run("AlreadyCatchUp", func(t *testing.T) {
		c := new(MockEthRpcClient)
		c.On("BlockHeaderByNumber", mock.Anything, ethTypes.FinalizedBlockNumber).
			Return(&ethTypes.Block{Number: big.NewInt(90)}, nil)

		conf := EthConfig{SerialOption: parallel.SerialOption{Routines: 2}, StartBlockNumber: 98}
		ex := newMockExtractor(conf, c, nil)

		dataChan := NewEthMemoryBoundedChannel(math.MaxInt)
		err, done := ex.catchUpOnce(context.Background(), dataChan)
		assert.NoError(t, err)
		assert.True(t, done)
	})
}

func TestAppendBlockHash(t *testing.T) {
	hashCache := NewBlockHashCache(0)
	c := new(MockEthRpcClient)
	conf := EthConfig{}
	ex := newMockExtractor(conf, c, hashCache)

	err := ex.AppendBlockHash(98, common.HexToHash("0x98"))
	assert.NoError(t, err)

	bn, bh, ok := hashCache.Latest()
	assert.True(t, ok)
	assert.Equal(t, uint64(98), bn)
	assert.Equal(t, common.HexToHash("0x98"), bh)

	err = ex.AppendBlockHash(100, common.HexToHash("0x100"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not continuous")
}
