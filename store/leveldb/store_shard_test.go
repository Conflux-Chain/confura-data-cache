package leveldb

import (
	"bytes"
	"math/big"
	"os"
	"testing"

	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	viperUtil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/ethereum/go-ethereum/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

const defaultTestShardingBlocks = 10

func createTestshardingStore(t *testing.T, defaultNextBlockNumber ...uint64) (*ShardingStore, func()) {
	path, err := os.MkdirTemp("", "confura-data-cache-")
	assert.Nil(t, err)

	config := ShardingConfig{
		Config: Config{
			Path: path,
		},
		ShardingBlocks: defaultTestShardingBlocks,
	}

	if len(defaultNextBlockNumber) > 0 {
		config.DefaultNextBlockNumber = defaultNextBlockNumber[0]
	}

	store, err := NewShardingStore(config)
	assert.Nil(t, err)

	return store, func() {
		store.Close()
		os.RemoveAll(path)
	}
}

func TestShardingConfig(t *testing.T) {
	defer viper.Reset()

	viper.SetConfigType("yml")
	viper.ReadConfig(bytes.NewBufferString(`
store:
  leveldb:
    path: db6
    shardingBlocks: 66
`))

	var config ShardingConfig
	viperUtil.MustUnmarshalKey("store.leveldb", &config)

	assert.Equal(t, "db6", config.Path)
	assert.Equal(t, uint64(66), config.ShardingBlocks)
}

func TestShardingStoreWrite(t *testing.T) {
	store, close := createTestshardingStore(t)
	defer close()

	earlist, ok := store.EarlistBlockNumber()
	assert.Equal(t, uint64(0), earlist)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), store.NextBlockNumber())

	// write empty
	assert.Nil(t, store.Write())

	// write continuous blocks to create 2 data stores
	var datas []evm.BlockData
	var bn int64
	for ; bn < int64(defaultTestShardingBlocks+2); bn++ {
		datas = append(datas, createTestEthData(bn, common.BigToHash(big.NewInt(bn))))
	}
	assert.Nil(t, store.Write(datas...))

	// cannot write again
	last := bn - 1
	assert.Error(t, store.Write(createTestEthData(last, common.BigToHash(big.NewInt(last)))))

	// cannot write future block
	future := bn + 1
	assert.Error(t, store.Write(createTestEthData(future, common.BigToHash(big.NewInt(future)))))

	// check earlist block number
	earlist, ok = store.EarlistBlockNumber()
	assert.True(t, ok)
	assert.Equal(t, uint64(0), earlist)

	// check next block number
	assert.Equal(t, uint64(bn), store.NextBlockNumber())
}

func TestShardingStoreBreakPoint(t *testing.T) {
	path, err := os.MkdirTemp("", "confura-data-cache-")
	assert.Nil(t, err)

	// starts from block 27
	store, err := NewShardingStore(ShardingConfig{Config{path, 27}, defaultTestShardingBlocks})
	assert.Nil(t, err)
	assert.Equal(t, uint64(27), store.NextBlockNumber())

	// write 5 continuous blocks to create 2 data stores:
	// shard 3: B27, B28, B29
	// shard 4: B30, B31
	var datas []evm.BlockData
	for bn := int64(27); bn <= int64(31); bn++ {
		datas = append(datas, createTestEthData(bn, common.BigToHash(big.NewInt(bn))))
	}
	assert.Nil(t, store.Write(datas...))

	// close store
	assert.Nil(t, store.Close())

	// reopen db again
	store, err = NewShardingStore(ShardingConfig{Config{path, 0}, defaultTestShardingBlocks})
	assert.Nil(t, err)
	defer store.Close()

	// earlist block is 27
	earlist, ok := store.EarlistBlockNumber()
	assert.True(t, ok)
	assert.Equal(t, uint64(27), earlist)

	// next block is 32
	assert.Equal(t, uint64(32), store.NextBlockNumber())
}
