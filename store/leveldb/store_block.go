package leveldb

import (
	"encoding/binary"

	"github.com/Conflux-Chain/confura-data-cache/types"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeBlock(batch *leveldb.Batch, block *ethTypes.Block) {
	// block hash -> block number
	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], block.Number.Uint64())
	store.write(batch, store.keyBlockHash2NumberPool, block.Hash.Bytes(), blockNumberBuf[:])

	// block number -> block
	store.writeJson(batch, store.keyBlockNumber2BlockPool, blockNumberBuf[:], block)

	// block number -> transaction count
	var txCountBuf [8]byte
	binary.BigEndian.PutUint64(txCountBuf[:], uint64(len(block.Transactions.Transactions())))
	store.write(batch, store.keyBlockNumber2TxCountPool, blockNumberBuf[:], txCountBuf[:])
}

// GetBlockNumber returns block number for the given block hash or number if any.
func (store *Store) GetBlockNumber(bhon types.BlockHashOrNumber) (uint64, bool, error) {
	hash, ok, number := bhon.HashOrNumber()
	if !ok {
		return number, true, nil
	}

	value, ok, err := store.read(store.keyBlockHash2NumberPool, hash.Bytes(), 8)
	if err != nil {
		return 0, false, errors.WithMessage(err, "Failed to get block number by hash")
	}

	if !ok {
		return 0, false, nil
	}

	return binary.BigEndian.Uint64(value), true, nil
}

// GetBlock returns block for the given block hash or number. If not found, returns nil.
func (store *Store) GetBlock(bhon types.BlockHashOrNumber) (types.Lazy[*ethTypes.Block], error) {
	number, ok, err := store.GetBlockNumber(bhon)
	if err != nil || !ok {
		return types.Lazy[*ethTypes.Block]{}, err
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], number)

	data, ok, err := store.read(store.keyBlockNumber2BlockPool, blockNumberBuf[:])
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
