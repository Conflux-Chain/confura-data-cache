package rpc

import (
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	lru "github.com/hashicorp/golang-lru/v2"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var _ Interface = (*Api)(nil)

// Api is the eth RPC implementation.
type Api struct {
	*leveldb.Store

	blockSummaryCache *lru.Cache[uint64, types.Lazy[*ethTypes.Block]]
}

func NewApi(store *leveldb.Store, lruCacheSize int) *Api {
	cache, _ := lru.New[uint64, types.Lazy[*ethTypes.Block]](lruCacheSize)
	return &Api{store, cache}
}

func (api *Api) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
	// try to return block summary from cache
	if !isFull {
		if _, ok, number := bhon.HashOrNumber(); !ok {
			if block, ok := api.blockSummaryCache.Get(number); ok {
				return block, nil
			}
		}
	}

	blockLazy, err := api.Store.GetBlock(bhon)
	if err != nil {
		return types.Lazy[*ethTypes.Block]{}, err
	}

	if isFull {
		return blockLazy, nil
	}

	// construct tx hash list
	block, err := blockLazy.Load()
	if err != nil {
		return types.Lazy[*ethTypes.Block]{}, errors.WithMessage(err, "Failed to unmarshal block")
	}

	if block == nil {
		return types.Lazy[*ethTypes.Block]{}, nil
	}

	txs := block.Transactions.Transactions()
	if txs == nil {
		block.Transactions = *ethTypes.NewTxOrHashListByHashes(nil)
	} else {
		hashes := make([]common.Hash, 0, len(txs))
		for _, v := range block.Transactions.Transactions() {
			hashes = append(hashes, v.Hash)
		}

		block.Transactions = *ethTypes.NewTxOrHashListByHashes(hashes)
	}

	result, err := types.NewLazy(block)
	if err != nil {
		return types.Lazy[*ethTypes.Block]{}, errors.WithMessage(err, "Failed to create lazy block summary")
	}

	api.blockSummaryCache.Add(block.Number.Uint64(), result)

	return result, nil
}

func (api *Api) GetTransactionByHash(txHash common.Hash) (types.Lazy[*ethTypes.TransactionDetail], error) {
	tx, err := api.Store.GetTransactionByHash(txHash)
	if err != nil {
		return types.Lazy[*ethTypes.TransactionDetail]{}, err
	}

	return types.NewLazy(tx)
}

func (api *Api) GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (types.Lazy[*ethTypes.TransactionDetail], error) {
	tx, err := api.Store.GetTransactionByIndex(bhon, txIndex)
	if err != nil {
		return types.Lazy[*ethTypes.TransactionDetail]{}, err
	}

	return types.NewLazy(tx)
}

func (api *Api) GetTransactionReceipt(txHash common.Hash) (types.Lazy[*ethTypes.Receipt], error) {
	receipt, err := api.Store.GetTransactionReceipt(txHash)
	if err != nil {
		return types.Lazy[*ethTypes.Receipt]{}, err
	}

	return types.NewLazy(receipt)
}

func (api *Api) GetTransactionTraces(txHash common.Hash) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	traces, err := api.Store.GetTransactionTraces(txHash)
	if err != nil {
		return types.Lazy[[]ethTypes.LocalizedTrace]{}, nil
	}

	return types.NewLazy(traces)
}
