package leveldb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeBlock(batch *leveldb.Batch, block *types.Block) {
	// block hash -> block number
	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], block.Number.Uint64())
	store.write(batch, store.keyBlockHash2NumberPool, block.Hash.Bytes(), blockNumberBuf[:])

	// block number -> block
	store.writeJson(batch, store.keyBlockNumber2BlockPool, blockNumberBuf[:], block)
}

// getBlockNumberByHash returns block number for the given block hash if any.
func (store *Store) getBlockNumberByHash(hash common.Hash) (uint64, bool, error) {
	value, ok, err := store.read(store.keyBlockHash2NumberPool, hash.Bytes(), 8)
	if err != nil {
		return 0, false, errors.WithMessage(err, "Failed to get block number by hash")
	}

	if !ok {
		return 0, false, nil
	}

	return binary.BigEndian.Uint64(value), true, nil
}

// GetBlockByHash returns block for the given block hash. If not found, returns nil.
func (store *Store) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	number, ok, err := store.getBlockNumberByHash(hash)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	return store.GetBlockByNumber(number)
}

// GetBlockByNumber returns block for the given block number. If not found, returns nil.
func (store *Store) GetBlockByNumber(number uint64) (*types.Block, error) {
	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], number)

	var block types.Block
	ok, err := store.readJson(store.keyBlockNumber2BlockPool, blockNumberBuf[:], &block)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	return &block, nil
}
