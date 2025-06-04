package leveldb

import (
	"encoding/binary"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeReceipts(batch *leveldb.Batch, blockNumber uint64, receipts []*ethTypes.Receipt) {
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

	receiptsLazy, err := store.GetBlockReceipts(types.BlockHashOrNumberWithNumber(blockNumber))
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get block receipts by number %v", blockNumber)
	}

	receipts, err := receiptsLazy.Load()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to unmarshal receipts")
	}

	if int(txIndex) >= len(receipts) {
		return nil, errors.Errorf("Data corrupted, invalid transaction index %v of length %v", txIndex, len(receipts))
	}

	return &receipts[txIndex], nil
}

// GetBlockReceipts returns all block receipts for the given block hash or number. If not found, returns nil.
func (store *Store) GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error) {
	blockNumber, ok, err := store.GetBlockNumber(bhon)
	if err != nil || !ok {
		return types.Lazy[[]ethTypes.Receipt]{}, err
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	data, ok, err := store.read(store.keyBlockNumber2ReceiptsPool, blockNumberBuf[:])
	if err != nil || !ok {
		return types.Lazy[[]ethTypes.Receipt]{}, err
	}

	return types.NewLazyWithJson[[]ethTypes.Receipt](data), nil
}
