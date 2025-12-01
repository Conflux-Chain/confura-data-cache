package leveldb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	indexStoreName  = "index"
	dataStorePrefix = "data"
)

var keyShardingMetadata = []byte("ShardingMetadata")

type ShardingConfig struct {
	Config `mapstructure:",squash"`

	ShardingBlocks uint64 // number of blocks in a shard
}

type ShardingMetadata struct {
	BlocksPerShard uint64
	EarlistShard   uint64 // inclusive, starts from 1, 0 indicates unavailable
	LatestShard    uint64 // inclusive, starts from 1, 0 indicates unavailable
}

func (metadata *ShardingMetadata) Validate(shardingBlocks uint64) error {
	if metadata.BlocksPerShard != shardingBlocks {
		return fmt.Errorf("Sharding blocks mismatch, expected = %v, actual = %v", shardingBlocks, metadata.BlocksPerShard)
	}

	if metadata.EarlistShard == 0 {
		return fmt.Errorf("EarlistShard is 0")
	}

	if metadata.EarlistShard > metadata.LatestShard {
		return fmt.Errorf("Invalid shard range, earlist = %v, latest = %v", metadata.EarlistShard, metadata.LatestShard)
	}

	return nil
}

// ShardingStore stores blockchain data in a sharding manner based on block number range.
type ShardingStore struct {
	config     ShardingConfig
	metadata   atomic.Value
	indexStore *Store   // used to index block hash and tx hash
	dataStores sync.Map // shard id => *Store, where shard id starts from 1
}

// NewShardingStore open or create a sharding store for the given path.
func NewShardingStore(config ShardingConfig) (*ShardingStore, error) {
	if config.ShardingBlocks == 0 {
		return nil, errors.New("ShardingBlocks is 0")
	}

	// open or create index database
	indexStore, err := NewStore(Config{Path: filepath.Join(config.Path, indexStoreName)})
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to open or create index store")
	}

	// load metadata
	var metadata ShardingMetadata
	found, err := indexStore.readJson(keyShardingMetadata, &metadata)
	if err != nil {
		indexStore.Close()
		return nil, errors.WithMessage(err, "Failed to load sharding metadata from index store")
	}

	// validate metadata if any
	if !found {
		metadata.BlocksPerShard = config.ShardingBlocks
	} else if err = metadata.Validate(config.ShardingBlocks); err != nil {
		indexStore.Close()
		return nil, errors.WithMessage(err, "Sharding metadata corrupted")
	}

	// init sharding store
	store := ShardingStore{
		config:     config,
		indexStore: indexStore,
	}
	store.metadata.Store(metadata)

	if !found {
		return &store, nil
	}

	// open data stores or return error if any data store missed
	for i := metadata.EarlistShard; i <= metadata.LatestShard; i++ {
		dataStore, err := NewStore(
			Config{Path: filepath.Join(config.Path, fmt.Sprintf("%v%v", dataStorePrefix, i))},
			opt.Options{ErrorIfMissing: true},
		)

		if err != nil {
			store.Close()
			return nil, errors.WithMessagef(err, "Failed to open data store of shard %v", i)
		}

		store.dataStores.Store(i, dataStore)
	}

	return &store, nil
}

// Close closes the underlying LevelDB databases.
func (store *ShardingStore) Close() error {
	if err := store.indexStore.Close(); err != nil {
		return errors.WithMessage(err, "Failed to close index database")
	}

	store.dataStores.Range(func(key, value any) bool {
		value.(*Store).Close()
		return true
	})

	return nil
}

// EarlistBlockNumber returns the earlist block number in database if any.
func (store *ShardingStore) EarlistBlockNumber() (uint64, bool) {
	metadata := store.metadata.Load().(ShardingMetadata)

	if val, ok := store.dataStores.Load(metadata.EarlistShard); ok {
		return val.(*Store).EarlistBlockNumber()
	}

	return 0, false
}

// NextBlockNumber returns the next block number to write in sequence.
func (store *ShardingStore) NextBlockNumber() uint64 {
	metadata := store.metadata.Load().(ShardingMetadata)

	if val, ok := store.dataStores.Load(metadata.LatestShard); ok {
		return val.(*Store).NextBlockNumber()
	}

	return store.config.DefaultNextBlockNumber
}

// Write writes the given block data in batch. It will return error if block data not written in sequence.
//
// Note, this method is not thread safe!
func (store *ShardingStore) Write(data ...types.EthBlockData) error {
	shardedData, err := store.parseShardingEthBlockData(data)
	if err != nil {
		return err
	}

	for _, v := range shardedData {
		if err = store.write(v); err != nil {
			return errors.WithMessagef(err, "Failed to write data of shard %v", v.Shard)
		}
	}

	return nil
}

type shardingEthBlockData struct {
	Shard uint64
	Datas []types.EthBlockData
}

func (store *ShardingStore) parseShardingEthBlockData(data []types.EthBlockData) ([]shardingEthBlockData, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var result []shardingEthBlockData

	expectedBlockNumber := data[0].Block.Number.Uint64()
	current := shardingEthBlockData{
		Shard: store.blockNumber2Shard(expectedBlockNumber),
	}

	for _, v := range data {
		if bn := v.Block.Number.Uint64(); bn != expectedBlockNumber {
			return nil, errors.Errorf("Block data not written in sequence, expected = %v, actual = %v", expectedBlockNumber, bn)
		}

		shard := store.blockNumber2Shard(expectedBlockNumber)

		if current.Shard == shard {
			current.Datas = append(current.Datas, v)
		} else {
			result = append(result, current)

			current = shardingEthBlockData{
				Shard: shard,
				Datas: []types.EthBlockData{v},
			}
		}

		expectedBlockNumber++
	}

	result = append(result, current)

	return result, nil
}

// blockNumber2Shard converts block number to shard id, which starts from 1.
func (store *ShardingStore) blockNumber2Shard(blockNumber uint64) uint64 {
	return 1 + blockNumber%store.config.ShardingBlocks
}

func (store *ShardingStore) write(data shardingEthBlockData) error {
	// it's ok to write multiple times if any error occur when writing data store
	if err := store.writeIndex(data); err != nil {
		return errors.WithMessage(err, "Failed to write index store")
	}

	dataStore, err := store.loadOrCreateDataStore(data.Shard)
	if err != nil {
		return errors.WithMessage(err, "Failed to create new data store")
	}

	if err = dataStore.Write(data.Datas...); err != nil {
		return errors.WithMessage(err, "Failed to write data into data store")
	}

	return nil
}

func (store *ShardingStore) writeIndex(data shardingEthBlockData) error {
	batch := new(leveldb.Batch)

	var shardBuf [8]byte
	binary.BigEndian.PutUint64(shardBuf[:], data.Shard)

	for _, data := range data.Datas {
		batch.Put(data.Block.Hash.Bytes(), shardBuf[:])

		for _, tx := range data.Block.Transactions.Transactions() {
			batch.Put(tx.Hash.Bytes(), shardBuf[:])
		}
	}

	return store.indexStore.db.Write(batch, nil)
}

func (store *ShardingStore) loadOrCreateDataStore(shard uint64) (*Store, error) {
	val, ok := store.dataStores.Load(shard)
	if ok {
		return val.(*Store), nil
	}

	nextBlockNumber := store.NextBlockNumber()

	// create new data store or open existing one due to lastest failure
	dataStore, err := NewStore(Config{
		Path:                   filepath.Join(store.config.Path, fmt.Sprintf("%v%v", dataStorePrefix, shard)),
		DefaultNextBlockNumber: nextBlockNumber,
	})
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to create data store")
	}

	// write metadata in index store
	metadata := store.metadata.Load().(ShardingMetadata)
	metadata.LatestShard = shard
	jsonMetadata, _ := json.Marshal(metadata)
	if err = store.indexStore.db.Put(keyShardingMetadata, jsonMetadata, nil); err != nil {
		return nil, errors.WithMessage(err, "Failed to update metadata in index store")
	}

	// update in-memory variables
	store.dataStores.Store(shard, dataStore)
	store.metadata.Store(metadata)

	logrus.WithFields(logrus.Fields{
		"shard":           shard,
		"nextBlockNumber": nextBlockNumber,
	}).Info("New data store created")

	return dataStore, nil
}
