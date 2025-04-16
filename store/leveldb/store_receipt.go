package leveldb

import (
	"encoding/binary"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeReceipts(batch *leveldb.Batch, blockNumber uint64, receipts []ethTypes.Receipt) {
	if receipts == nil {
		return
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	store.writeJson(batch, store.keyBlockNumber2ReceiptsPool, blockNumberBuf[:], receipts)
}

// GetTransactionReceipt returns receipt for the given transaction hash. If not found, returns nil.
func (store *Store) GetTransactionReceipt(txHash common.Hash) (*ethTypes.Receipt, error) {
	blockNumber, txIndex, ok, err := store.getBlockNumberAndIndexByTransactionHash(txHash)
	if err != nil || !ok {
		return nil, err
	}

	receiptsLazy, err := store.GetBlockReceiptsByNumber(blockNumber)
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get block receipts by number %v", blockNumber)
	}

	receipts := receiptsLazy.MustLoad()
	if int(txIndex) >= len(receipts) {
		return nil, errors.Errorf("Data corrupted, invalid transaction index %v of length %v", txIndex, len(receipts))
	}

	return &receipts[txIndex], nil
}

// GetBlockReceiptsByHash returns all block receipts for the given block hash. If not found, returns nil.
func (store *Store) GetBlockReceiptsByHash(blockHash common.Hash) (types.Lazy[[]ethTypes.Receipt], error) {
	blockNumber, ok, err := store.getBlockNumberByHash(blockHash)
	if err != nil || !ok {
		return types.Lazy[[]ethTypes.Receipt]{}, err
	}

	return store.GetBlockReceiptsByNumber(blockNumber)
}

// GetBlockReceiptsByNumber returns all block receipts for the given block number. If not found, returns nil.
func (store *Store) GetBlockReceiptsByNumber(blockNumber uint64) (types.Lazy[[]ethTypes.Receipt], error) {
	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	var receipts types.Lazy[[]ethTypes.Receipt]
	ok, err := store.readJson(store.keyBlockNumber2ReceiptsPool, blockNumberBuf[:], &receipts)
	if err != nil || !ok {
		return types.Lazy[[]ethTypes.Receipt]{}, err
	}

	return receipts, nil
}
