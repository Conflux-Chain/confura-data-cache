package extract

import (
	"context"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
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

	extractor, err := NewEvmExtractor(conf)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataChan := make(chan types.EthBlockData, 1)
	go extractor.Start(ctx, dataChan)

	data := <-dataChan
	assert.NotNil(t, data)
	assert.NotNil(t, data.Block)
	assert.NotNil(t, data.Receipts)
	assert.NotNil(t, data.Traces)
}

func TestNewEvmExtractor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      EthConfig
		expectError bool
		errorText   string
	}{
		{"NoRpcEndpoint", EthConfig{}, true, "no rpc endpoint provided"},
		{"InvalidRpcEndpoint", EthConfig{RpcEndpoint: "invalid"}, true, "failed to create rpc client"},
		{"ValidConfig", EthConfig{RpcEndpoint: "http://localhost:8545"}, false, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ext, err := NewEvmExtractor(tc.config)
			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.errorText)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, ext)
			}
		})
	}

	t.Run("CustomFinalizedProvider", func(t *testing.T) {
		cfg := EthConfig{TargetBlockNumber: ethTypes.FinalizedBlockNumber, RpcEndpoint: "http://localhost:8545"}
		c := new(MockEthRpcClient)
		c.On("BlockHeaderByNumber", mock.Anything, cfg.TargetBlockNumber).Return((*ethTypes.Block)(nil), errors.New("rpc error"))
		ex, err := NewEvmExtractor(cfg, newEthFinalizedHeightProvider(c))
		assert.NoError(t, err)

		err = ex.hashCache.Append(1, common.HexToHash("0x1"))
		assert.ErrorContains(t, err, "failed to get finalized block number")
	})
}

func TestEthExtractorExtractOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	block := makeMockBlock(100, "0x100", "0x99")

	tests := []struct {
		name    string
		setup   func(*MockEthRpcClient)
		cfg     EthConfig
		cache   *BlockHashCache
		wantErr string
		caught  bool
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
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := new(MockEthRpcClient)
			tc.setup(c)
			ex := newMockExtractor(tc.cfg, c, tc.cache)
			_, caughtUp, err := ex.extractOnce(ctx)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.caught, caughtUp)
		})
	}
}

func TestEthExtractorStart(t *testing.T) {
	t.Run("ContextCancellation", func(t *testing.T) {
		c := new(MockEthRpcClient)
		c.On("Close").Return(nil)

		ctx, cancel := context.WithCancel(context.Background())
		ex := newMockExtractor(EthConfig{PollInterval: time.Millisecond}, c, nil)
		dataChan := make(chan types.EthBlockData)

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

		cache := NewBlockHashCache(0)
		cache.Append(99, common.HexToHash("0x99"))

		ex := newMockExtractor(EthConfig{
			TargetBlockNumber: ethTypes.LatestBlockNumber,
			StartBlockNumber:  100,
			PollInterval:      time.Millisecond,
		}, c, cache)

		dataChan := make(chan types.EthBlockData, 1)
		go ex.Start(ctx, dataChan)

		select {
		case data := <-dataChan:
			assert.Equal(t, uint64(100), data.Block.Number.Uint64())
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Timed out waiting for block data")
		}
	})
}
