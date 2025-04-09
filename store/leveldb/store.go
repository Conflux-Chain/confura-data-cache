package leveldb

import (
	"encoding/binary"
	"encoding/json"
	"sync/atomic"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	dberrors "github.com/syndtr/goleveldb/leveldb/errors"
)

var keyNextBlockNumber = []byte("nextBlockNumber")

// Store provides operations on a LevelDB database.
type Store struct {
	db *leveldb.DB

	// next block number to write in sequence
	nextBlockNumber atomic.Uint64

	// use object pool for memory saving
	keyBlockHash2NumberPool  *KeyPool
	keyBlockNumber2BlockPool *KeyPool
}

// NewStore opens or creates a DB for the given path.
//
// If corruption detected for an existing DB, it will try to recover the DB.
func NewStore(path string) (*Store, error) {
	// open or create database
	db, err := leveldb.OpenFile(path, nil)
	if dberrors.IsCorrupted(err) {
		// try to recover database
		logrus.WithError(err).WithField("path", path).Warn("Failed to open corrupted file, try to recover")
		db, err = leveldb.RecoverFile(path, nil)
		if err != nil {
			return nil, errors.WithMessagef(err, "Failed to recover file %v", path)
		}
	} else if err != nil {
		return nil, errors.WithMessagef(err, "Failed to open file %v", path)
	}

	store := Store{
		db: db,

		keyBlockHash2NumberPool:  NewKeyPool("bh", 32),
		keyBlockNumber2BlockPool: NewKeyPool("bn", 8),
	}

	// init next block number to write
	nextBlockNumber, ok, err := store.getNextBlockNumber()
	if err != nil {
		db.Close()
		return nil, errors.WithMessage(err, "Failed to get next block number")
	}

	if ok {
		store.nextBlockNumber.Store(nextBlockNumber)
	}

	return &store, nil
}

// getNextBlockNumber returns the next block number in database.
func (store *Store) getNextBlockNumber() (uint64, bool, error) {
	value, err := store.db.Get(keyNextBlockNumber, nil)
	if err == nil {
		return binary.BigEndian.Uint64(value), true, nil
	}

	if err == dberrors.ErrNotFound {
		return 0, false, nil
	}

	return 0, false, err
}

// Close closes the underlying LevelDB database.
func (store *Store) Close() error {
	return store.db.Close()
}

// NextBlockNumber returns the next block number to write in sequence.
func (store *Store) NextBlockNumber() uint64 {
	return store.nextBlockNumber.Load()
}

// Write writes the given block data in batch. It will return error if block data not written in sequence.
//
// Note, this method is not thread safe!
func (store *Store) Write(data types.EthBlockData) error {
	// ensure block data written in sequence
	blockNumber := data.Block.Number.Uint64()
	if next := store.nextBlockNumber.Load(); next != blockNumber {
		return errors.Errorf("Block data not written in sequence, expected = %v, actual = %v", next, blockNumber)
	}

	batch := new(leveldb.Batch)

	// block number
	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	// block hash -> block number
	keyBlockHash2Number := store.keyBlockHash2NumberPool.Get(data.Block.Hash.Bytes())
	defer store.keyBlockHash2NumberPool.Put(keyBlockHash2Number)
	batch.Put(*keyBlockHash2Number, blockNumberBuf[:])

	// block number -> block
	keyBlockNumber2Block := store.keyBlockNumber2BlockPool.Get(blockNumberBuf[:])
	defer store.keyBlockNumber2BlockPool.Put(keyBlockNumber2Block)
	blockJson, _ := json.Marshal(data.Block)
	batch.Put(*keyBlockNumber2Block, blockJson)

	// TODO write txs
	// TODO write receipts
	// TODO write traces

	// update next block to write in sequence
	var nextBlockNumberBuf [8]byte
	binary.BigEndian.PutUint64(nextBlockNumberBuf[:], blockNumber+1)
	batch.Put(keyNextBlockNumber, nextBlockNumberBuf[:])

	if err := store.db.Write(batch, nil); err != nil {
		return err
	}

	store.nextBlockNumber.Add(1)

	return nil
}

// GetBlockByHash returns block for the given block hash. If not found, returns nil.
func (store *Store) GetBlockByHash(hash common.Hash) (*ethTypes.Block, error) {
	keyBlockHash2Number := store.keyBlockHash2NumberPool.Get(hash.Bytes())
	defer store.keyBlockHash2NumberPool.Put(keyBlockHash2Number)

	value, err := store.db.Get(*keyBlockHash2Number, nil)
	if err == dberrors.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get block number by hash")
	}

	number := binary.BigEndian.Uint64(value)

	return store.GetBlockByNumber(number)
}

// GetBlockByNumber returns block for the given block number. If not found, returns nil.
func (store *Store) GetBlockByNumber(number uint64) (*ethTypes.Block, error) {
	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], number)

	keyBlockNumber2Block := store.keyBlockNumber2BlockPool.Get(blockNumberBuf[:])
	defer store.keyBlockNumber2BlockPool.Put(keyBlockNumber2Block)

	value, err := store.db.Get(*keyBlockNumber2Block, nil)
	if err == dberrors.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	var block ethTypes.Block
	if err = json.Unmarshal(value, &block); err != nil {
		return nil, errors.WithMessage(err, "Failed to unmarshal block")
	}

	return &block, nil
}
