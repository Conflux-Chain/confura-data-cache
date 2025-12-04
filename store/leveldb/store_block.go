package leveldb

import (
	"encoding/binary"

	"github.com/Conflux-Chain/confura-data-cache/types"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeBlock(batch *leveldb.Batch, data types.EthBlockData) {
	// block hash -> block number
	var blockNumberBuf [8]byte
	block := data.Block
	binary.BigEndian.PutUint64(blockNumberBuf[:], block.Number.Uint64())
	store.write(batch, store.keyBlockHash2NumberPool, block.Hash.Bytes(), blockNumberBuf[:])

	// block number -> block
	store.writeJson(batch, store.keyBlockNumber2BlockPool, blockNumberBuf[:], block)

	// block number -> block summary
	blockSummary := data.BlockSummary()
	store.writeJson(batch, store.keyBlockNumber2BlockSummaryPool, blockNumberBuf[:], blockSummary)

	// block number -> transaction count
	var txCountBuf [8]byte
	binary.BigEndian.PutUint64(txCountBuf[:], uint64(len(block.Transactions.Transactions())))
	store.write(batch, store.keyBlockNumber2TxCountPool, blockNumberBuf[:], txCountBuf[:])
}

func (store *Store) isBlockNumberInRange(bn uint64) bool {
	earlist, ok := store.EarlistBlockNumber()

	return ok && bn >= earlist && bn < store.NextBlockNumber()
}

// GetBlockNumber returns block number for the given block hash or number if any.
func (store *Store) GetBlockNumber(bhon types.BlockHashOrNumber) (uint64, bool, error) {
	hash, ok, number := bhon.HashOrNumber()
	if !ok {
		if store.isBlockNumberInRange(number) {
			return number, true, nil
		}

		return 0, false, nil
	}

	value, ok, err := store.read(store.keyBlockHash2NumberPool, hash.Bytes(), 8)
	if err != nil {
		return 0, false, errors.WithMessage(err, "Failed to get block number by hash")
	}

	if !ok {
		return 0, false, nil
	}

	number = binary.BigEndian.Uint64(value)

	if store.isBlockNumberInRange(number) {
		return number, true, nil
	}

	return 0, false, nil
}

// GetBlock returns block for the given block hash or number. If not found, returns nil.
func (store *Store) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
	number, ok, err := store.GetBlockNumber(bhon)
	if err != nil || !ok {
		return types.Lazy[*ethTypes.Block]{}, err
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], number)

	keyPool := store.keyBlockNumber2BlockSummaryPool
	if isFull {
		keyPool = store.keyBlockNumber2BlockPool
	}

	data, ok, err := store.read(keyPool, blockNumberBuf[:])
	if err != nil || !ok {
		return types.Lazy[*ethTypes.Block]{}, err
	}

	return types.NewLazyWithJson[*ethTypes.Block](data), nil
}

// GetBlockTransactionCount returns the transaction count for the given block hash or number.
// Returns -1 if the given block hash not found.
func (store *Store) GetBlockTransactionCount(bhon types.BlockHashOrNumber) (int64, error) {
	blockNumber, ok, err := store.GetBlockNumber(bhon)
	if err != nil || !ok {
		return -1, err
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	value, ok, err := store.read(store.keyBlockNumber2TxCountPool, blockNumberBuf[:], 8)
	if err != nil || !ok {
		return -1, err
	}

	count := binary.BigEndian.Uint64(value)

	return int64(count), nil
}
