package leveldb

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/Conflux-Chain/go-conflux-util/blockchain/sync/evm"
	"github.com/mcuadros/go-defaults"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
	dberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	keyEarlistBlockNumber = []byte("earlistBlockNumber")
	keyNextBlockNumber    = []byte("nextBlockNumber")
)

// Config holds the configurations for LevelDB database.
type Config struct {
	// LevelDB database path.
	Path string `default:"db"`

	// DefaultNextBlockNumber is the block number to write next block if database is empty.
	DefaultNextBlockNumber uint64
}

func DefaultConfig() (config Config) {
	defaults.SetDefaults(&config)
	return
}

// Store provides operations on a LevelDB database.
type Store struct {
	db *leveldb.DB

	// earlist block number in databae, -1 indicates empty database
	earlistBlockNumber atomic.Int64

	// next block number to write in sequence
	nextBlockNumber atomic.Uint64

	// use object pool for memory saving
	keyBlockHash2NumberPool           *KeyPool
	keyBlockNumber2BlockPool          *KeyPool
	keyBlockNumber2BlockSummaryPool   *KeyPool
	keyBlockNumber2TxCountPool        *KeyPool
	keyTxHash2BlockNumberAndIndexPool *KeyPool
	keyBlockNumber2ReceiptsPool       *KeyPool
	keyBlockNumber2TracesPool         *KeyPool

	metrics Metrics
}

// NewStore opens or creates a DB for the given path.
//
// If corruption detected for an existing DB, it will try to recover the DB.
//
// If the DB of specified path is empty and defaultNextBlockNumber specified,
// store will write data from defaultNextBlockNumber.
func NewStore(config Config, options ...opt.Options) (*Store, error) {
	var opt *opt.Options
	if len(options) > 0 {
		opt = &options[0]
	}

	// open or create database
	db, err := leveldb.OpenFile(config.Path, opt)
	if dberrors.IsCorrupted(err) {
		// try to recover database
		logrus.WithError(err).WithField("path", config.Path).Warn("Failed to open corrupted file, try to recover")
		db, err = leveldb.RecoverFile(config.Path, opt)
		if err != nil {
			return nil, errors.WithMessagef(err, "Failed to recover file %v", config.Path)
		}
	} else if err != nil {
		return nil, errors.WithMessagef(err, "Failed to open file %v", config.Path)
	}

	store := Store{
		db: db,

		keyBlockHash2NumberPool:           NewKeyPool("bh2bn", 32),
		keyBlockNumber2BlockPool:          NewKeyPool("bn2b", 8),
		keyBlockNumber2BlockSummaryPool:   NewKeyPool("bn2bs", 8),
		keyBlockNumber2TxCountPool:        NewKeyPool("bn2tc", 8),
		keyTxHash2BlockNumberAndIndexPool: NewKeyPool("th2bni", 32),
		keyBlockNumber2ReceiptsPool:       NewKeyPool("bn2rs", 8),
		keyBlockNumber2TracesPool:         NewKeyPool("bn2ts", 8),
	}

	// load the earlist block number
	earlist, ok, err := store.readUint64(keyEarlistBlockNumber)
	if err != nil {
		db.Close()
		return nil, errors.WithMessage(err, "Failed to load earlist block number in database")
	}

	if ok {
		store.earlistBlockNumber.Store(int64(earlist))
	} else {
		store.earlistBlockNumber.Store(-1)
	}

	// init next block number to write
	nextBlockNumber, ok, err := store.readUint64(keyNextBlockNumber)
	if err != nil {
		db.Close()
		return nil, errors.WithMessage(err, "Failed to load next block number in database")
	}

	if ok {
		store.nextBlockNumber.Store(nextBlockNumber)
	} else if config.DefaultNextBlockNumber > 0 {
		store.nextBlockNumber.Store(config.DefaultNextBlockNumber)
	}

	return &store, nil
}

// Close closes the underlying LevelDB database.
func (store *Store) Close() error {
	return store.db.Close()
}

// EarlistBlockNumber returns the earlist block number in database if any.
func (store *Store) EarlistBlockNumber() (uint64, bool) {
	if earlist := store.earlistBlockNumber.Load(); earlist != -1 {
		return uint64(earlist), true
	}

	return 0, false
}

// NextBlockNumber returns the next block number to write in sequence.
func (store *Store) NextBlockNumber() uint64 {
	return store.nextBlockNumber.Load()
}

// Write writes the given block data in batch. It will return error if block data not written in sequence.
//
// Note, this method is not thread safe!
func (store *Store) Write(data ...evm.BlockData) error {
	if len(data) == 0 {
		return nil
	}

	// ensure block data written in sequence
	next := store.nextBlockNumber.Load()
	for _, v := range data {
		if bn := v.Block.Number.Uint64(); bn != next {
			return errors.Errorf("Block data not written in sequence, expected = %v, actual = %v", next, bn)
		}

		next++
	}

	start := time.Now()
	batch := new(leveldb.Batch)

	for _, v := range data {
		blockNumber := v.Block.Number.Uint64()

		store.writeBlock(batch, v)
		store.writeTransactions(batch, v.Block.Transactions.Transactions())
		store.writeReceipts(batch, blockNumber, v.Receipts)
		store.writeTraces(batch, blockNumber, v.Traces)
	}

	// update next block to write in sequence
	var nextBlockNumberBuf [8]byte
	binary.BigEndian.PutUint64(nextBlockNumberBuf[:], next)
	batch.Put(keyNextBlockNumber, nextBlockNumberBuf[:])

	// write earlist block number for the first time
	if store.earlistBlockNumber.Load() == -1 {
		earlistBlockNumber := data[0].Block.Number.Uint64()
		var earlistBlockNumberBuf [8]byte
		binary.BigEndian.PutUint64(earlistBlockNumberBuf[:], earlistBlockNumber)
		batch.Put(keyEarlistBlockNumber, earlistBlockNumberBuf[:])
	}

	if err := store.db.Write(batch, nil); err != nil {
		return err
	}

	// update in-memory atomic variables
	store.nextBlockNumber.Store(next)
	if store.earlistBlockNumber.Load() == -1 {
		store.earlistBlockNumber.Store(data[0].Block.Number.Int64())
	}

	// add metrics
	store.metrics.Latest().Update(int64(next - 1))
	store.metrics.Write().UpdateSince(start)

	return nil
}
