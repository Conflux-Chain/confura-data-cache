package leveldb

import (
	"encoding/binary"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeTraces(batch *leveldb.Batch, blockNumber uint64, traces []ethTypes.LocalizedTrace) {
	if traces == nil {
		return
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	store.writeJson(batch, store.keyBlockNumber2TracesPool, blockNumberBuf[:], traces)
}

// GetTransactionTraces returns all transaction traces for the given transaction hash. If not found, returns nil.
func (store *Store) GetTransactionTraces(txHash common.Hash) ([]ethTypes.LocalizedTrace, error) {
	blockNumber, txIndex, ok, err := store.getBlockNumberAndIndexByTransactionHash(txHash)
	if err != nil || !ok {
		return nil, err
	}

	blockTracesLazy, err := store.GetBlockTraces(types.BlockHashOrNumberWithNumber(blockNumber))
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get block traces by number %v", blockNumber)
	}

	blockTraces, err := blockTracesLazy.Load()
	if err != nil {
		return nil, errors.WithMessage(err, "Failed to unmarshal block traces")
	}

	var txTraces []ethTypes.LocalizedTrace
	for _, trace := range blockTraces {
		if trace.TransactionPosition != nil && *trace.TransactionPosition == uint(txIndex) {
			txTraces = append(txTraces, trace)
		}
	}

	return txTraces, nil
}

// GetBlockTraces returns all block traces for the given block hash or number. If not found, returns nil.
func (store *Store) GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	blockNumber, ok, err := store.getBlockNumber(bhon)
	if err != nil || !ok {
		return types.Lazy[[]ethTypes.LocalizedTrace]{}, err
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	var traces types.Lazy[[]ethTypes.LocalizedTrace]
	ok, err = store.readJson(store.keyBlockNumber2TracesPool, blockNumberBuf[:], &traces)
	if err != nil || !ok {
		return types.Lazy[[]ethTypes.LocalizedTrace]{}, err
	}

	return traces, nil
}
