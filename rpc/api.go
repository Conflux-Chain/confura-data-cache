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

// GetBlock implements the `Interface` interface.
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
