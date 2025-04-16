package leveldb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeTransactions(batch *leveldb.Batch, transactions []types.TransactionDetail) {
	// Historical transaction will not be accessed in high QPS, so just read from the block txs body.
	// Only add necessary index for tx hash.
	for i, tx := range transactions {
		// block number: u64 + tx index: u32
		var value [12]byte
		binary.BigEndian.PutUint64(value[:8], tx.BlockNumber.Uint64())
		binary.BigEndian.PutUint32(value[8:], uint32(i))

		store.write(batch, store.keyTxHash2BlockNumberAndIndexPool, tx.Hash.Bytes(), value[:])
	}
}

func (store *Store) getBlockNumberAndIndexByTransactionHash(txHash common.Hash) (uint64, uint32, bool, error) {
	value, ok, err := store.read(store.keyTxHash2BlockNumberAndIndexPool, txHash.Bytes(), 12)
	if err != nil {
		return 0, 0, false, errors.WithMessage(err, "Failed to get block number and index by transaction hash")
	}

	if !ok {
		return 0, 0, false, nil
	}

	blockNumber := binary.BigEndian.Uint64(value[:8])
	txIndex := binary.BigEndian.Uint32(value[8:])

	return blockNumber, txIndex, true, nil
}

// GetTransactionByHash returns transaction for the given transaction hash. If not found, returns nil.
func (store *Store) GetTransactionByHash(txHash common.Hash) (*types.TransactionDetail, error) {
	blockNumber, txIndex, ok, err := store.getBlockNumberAndIndexByTransactionHash(txHash)
	if err != nil || !ok {
		return nil, err
	}

	return store.GetTransactionByBlockNumberAndIndex(blockNumber, txIndex)
}

// GetTransactionByBlockHashAndIndex returns transaction for the given block hash and transaction index. If not found, returns nil.
func (store *Store) GetTransactionByBlockHashAndIndex(blockHash common.Hash, txIndex uint32) (*types.TransactionDetail, error) {
	blockNumber, ok, err := store.getBlockNumberByHash(blockHash)
	if err != nil || !ok {
		return nil, err
	}

	return store.GetTransactionByBlockNumberAndIndex(blockNumber, txIndex)
}

// GetTransactionByBlockNumberAndIndex returns transaction for the given block number and transaction index. If not found, returns nil.
func (store *Store) GetTransactionByBlockNumberAndIndex(blockNumber uint64, txIndex uint32) (*types.TransactionDetail, error) {
	block, err := store.GetBlockByNumber(blockNumber)
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get block by number %v", blockNumber)
	}

	txs := block.MustLoad().Transactions.Transactions()

	if int(txIndex) >= len(txs) {
		return nil, nil
	}

	return &txs[txIndex], nil
}
