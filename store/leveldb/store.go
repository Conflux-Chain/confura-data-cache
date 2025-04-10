package leveldb

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/Conflux-Chain/confura-data-cache/types"
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
	keyBlockHash2NumberPool    *KeyPool
	keyBlockNumber2BlockPool   *KeyPool
	keyBlockNumber2TxCountPool *KeyPool
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

		keyBlockHash2NumberPool:    NewKeyPool("bh2bn", 32),
		keyBlockNumber2BlockPool:   NewKeyPool("bn2b", 8),
		keyBlockNumber2TxCountPool: NewKeyPool("bn2tc", 8),
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
	if err == dberrors.ErrNotFound {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	if len(value) != 8 {
		return 0, false, errors.Errorf("Invalid value size, expected = 8, actual = %v", len(value))
	}

	return binary.BigEndian.Uint64(value), true, nil
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

	store.writeBlock(batch, data.Block)

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
