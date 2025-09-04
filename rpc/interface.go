package rpc

import (
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
)

type Interface interface {
	// GetBlock returns block for the given block hash or number. If not found, returns nil.
	GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error)

	// GetBlockTransactionCount returns the transaction count for the given block hash or number. If not found, returns -1.
	GetBlockTransactionCount(bhon types.BlockHashOrNumber) (int64, error)

	// GetTransactionByHash returns transaction for the given transaction hash. If not found, returns nil.
	GetTransactionByHash(txHash common.Hash) (types.Lazy[*ethTypes.TransactionDetail], error)

	// GetTransactionByIndex returns transaction for the given block hash or number along with transaction index. If not found, returns nil.
	GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (types.Lazy[*ethTypes.TransactionDetail], error)

	// GetTransactionReceipt returns receipt for the given transaction hash. If not found, returns nil.
	GetTransactionReceipt(txHash common.Hash) (types.Lazy[*ethTypes.Receipt], error)

	// GetBlockReceipts returns all block receipts for the given block hash or number. If not found, returns nil.
	GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error)

	// GetTransactionTraces returns all transaction traces for the given transaction hash. If not found, returns nil.
	GetTransactionTraces(txHash common.Hash) (types.Lazy[[]ethTypes.LocalizedTrace], error)

	// GetBlockTraces returns all block traces for the given block hash or number. If not found, returns nil.
	GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error)

	// GetTrace returns single trace for the given transaction hash at specified index. If not found, returns nil.
	GetTrace(txHash common.Hash, index uint) (types.Lazy[*ethTypes.LocalizedTrace], error)
}

var NotFoundImpl Interface = notFoundImpl{}

type notFoundImpl struct{}

func (notFoundImpl) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
	return types.Lazy[*ethTypes.Block]{}, nil
}

func (notFoundImpl) GetBlockTransactionCount(bhon types.BlockHashOrNumber) (int64, error) {
	return -1, nil
}

func (notFoundImpl) GetTransactionByHash(txHash common.Hash) (types.Lazy[*ethTypes.TransactionDetail], error) {
	return types.Lazy[*ethTypes.TransactionDetail]{}, nil
}

func (notFoundImpl) GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (types.Lazy[*ethTypes.TransactionDetail], error) {
	return types.Lazy[*ethTypes.TransactionDetail]{}, nil
}

func (notFoundImpl) GetTransactionReceipt(txHash common.Hash) (types.Lazy[*ethTypes.Receipt], error) {
	return types.Lazy[*ethTypes.Receipt]{}, nil
}

func (notFoundImpl) GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error) {
	return types.Lazy[[]ethTypes.Receipt]{}, nil
}

func (notFoundImpl) GetTransactionTraces(txHash common.Hash) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	return types.Lazy[[]ethTypes.LocalizedTrace]{}, nil
}

func (notFoundImpl) GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	return types.Lazy[[]ethTypes.LocalizedTrace]{}, nil
}

func (notFoundImpl) GetTrace(txHash common.Hash, index uint) (types.Lazy[*ethTypes.LocalizedTrace], error) {
	return types.Lazy[*ethTypes.LocalizedTrace]{}, nil
}
