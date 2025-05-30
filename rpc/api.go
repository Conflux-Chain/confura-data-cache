package rpc

import (
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

var _ Interface = (*Api)(nil)

// Api is the eth RPC implementation.
type Api struct {
	*leveldb.Store
}

func NewApi(store *leveldb.Store) *Api {
	return &Api{store}
}

func (api *Api) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
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
		return types.NewLazy(block)
	}

	hashes := make([]common.Hash, 0, len(txs))
	for _, v := range block.Transactions.Transactions() {
		hashes = append(hashes, v.Hash)
	}

	block.Transactions = *ethTypes.NewTxOrHashListByHashes(hashes)

	return types.NewLazy(block)
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
