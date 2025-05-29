package leveldb

import (
	"encoding/binary"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeTransactions(batch *leveldb.Batch, transactions []ethTypes.TransactionDetail) {
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
func (store *Store) GetTransactionByHash(txHash common.Hash) (*ethTypes.TransactionDetail, error) {
	blockNumber, txIndex, ok, err := store.getBlockNumberAndIndexByTransactionHash(txHash)
	if err != nil || !ok {
		return nil, err
	}

	return store.GetTransactionByIndex(types.BlockHashOrNumberWithNumber(blockNumber), txIndex)
}

// GetTransactionByIndex returns transaction for the given block hash or number along with transaction index. If not found, returns nil.
func (store *Store) GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (*ethTypes.TransactionDetail, error) {
	blockLazy, err := store.GetBlock(bhon)
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to get block by hash or number")
	}

	if blockLazy.IsEmptyOrNull() {
		return nil, nil // No block found for this hash or number
	}

	block, err := blockLazy.Load()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to unmarshal block")
	}

	txs := block.Transactions.Transactions()

	if int(txIndex) >= len(txs) {
		return nil, nil
	}

	return &txs[txIndex], nil
}
