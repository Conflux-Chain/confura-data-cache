package leveldb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *Store) writeTraces(batch *leveldb.Batch, traces []types.LocalizedTrace) {
	if len(traces) == 0 {
		return
	}

	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], traces[0].BlockNumber)

	store.writeJson(batch, store.keyBlockNumber2TracesPool, blockNumberBuf[:], traces)
}

// GetTransactionTraces returns all transaction traces for the given transaction hash. If not found, returns nil.
func (store *Store) GetTransactionTraces(txHash common.Hash) ([]types.LocalizedTrace, error) {
	blockNumber, txIndex, ok, err := store.getBlockNumberAndIndexByTransactionHash(txHash)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	blockTraces, err := store.GetBlockTracesByNumber(blockNumber)
	if err != nil {
		return nil, errors.WithMessagef(err, "Failed to get block traces by number %v", blockNumber)
	}

	var txTraces []types.LocalizedTrace
	for _, trace := range blockTraces {
		if trace.TransactionPosition != nil && *trace.TransactionPosition == uint(txIndex) {
			txTraces = append(txTraces, trace)
		}
	}

	return txTraces, nil
}

// GetBlockTracesByHash returns all block traces for the given block hash. If not found, returns nil.
func (store *Store) GetBlockTracesByHash(blockHash common.Hash) ([]types.LocalizedTrace, error) {
	blockNumber, ok, err := store.getBlockNumberByHash(blockHash)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	return store.GetBlockTracesByNumber(blockNumber)
}

// GetBlockTracesByNumber returns all block traces for the given block number. If not found, returns nil.
func (store *Store) GetBlockTracesByNumber(blockNumber uint64) ([]types.LocalizedTrace, error) {
	var blockNumberBuf [8]byte
	binary.BigEndian.PutUint64(blockNumberBuf[:], blockNumber)

	var traces []types.LocalizedTrace
	ok, err := store.readJson(store.keyBlockNumber2TracesPool, blockNumberBuf[:], &traces)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	return traces, nil
}
