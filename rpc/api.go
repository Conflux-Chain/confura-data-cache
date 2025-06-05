package rpc

import (
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	lru "github.com/hashicorp/golang-lru/v2"
	ethTypes "github.com/openweb3/web3go/types"
)

var _ Interface = (*Api)(nil)

// Api is the eth RPC implementation.
type Api struct {
	*leveldb.Store

	blockCache        *lru.Cache[uint64, types.Lazy[*ethTypes.Block]]
	blockSummaryCache *lru.Cache[uint64, types.Lazy[*ethTypes.Block]]
}

func NewApi(store *leveldb.Store, lruCacheSize int) *Api {
	api := Api{Store: store}
	api.blockCache, _ = lru.New[uint64, types.Lazy[*ethTypes.Block]](lruCacheSize)
	api.blockSummaryCache, _ = lru.New[uint64, types.Lazy[*ethTypes.Block]](lruCacheSize)
	return &api
}

func (api *Api) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
	metrics.GetBlockIsFull().Mark(isFull)

	number, ok, err := api.Store.GetBlockNumber(bhon)
	if err != nil || !ok {
		return types.Lazy[*ethTypes.Block]{}, err
	}

	// try to load from cache
	cache := api.blockSummaryCache
	if isFull {
		cache = api.blockCache
	}

	cachedBlock, ok := cache.Get(number)
	metrics.GetBlockHitCache(isFull).Mark(ok)
	if ok {
		return cachedBlock, nil
	}

	// load from store
	block, err := api.Store.GetBlock(bhon, isFull)
	if err != nil {
		return types.Lazy[*ethTypes.Block]{}, err
	}

	// add into cache if found
	if !block.IsEmptyOrNull() {
		cache.Add(number, block)
	}

	return block, nil
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
