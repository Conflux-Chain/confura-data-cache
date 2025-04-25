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
	GetTransactionByHash(txHash common.Hash) (*ethTypes.TransactionDetail, error)

	// GetTransactionByIndex returns transaction for the given block hash or number along with transaction index. If not found, returns nil.
	GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (*ethTypes.TransactionDetail, error)

	// GetTransactionReceipt returns receipt for the given transaction hash. If not found, returns nil.
	GetTransactionReceipt(txHash common.Hash) (*ethTypes.Receipt, error)

	// GetBlockReceipts returns all block receipts for the given block hash or number. If not found, returns nil.
	GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error)

	// GetTransactionTraces returns all transaction traces for the given transaction hash. If not found, returns nil.
	GetTransactionTraces(txHash common.Hash) ([]ethTypes.LocalizedTrace, error)

	// GetBlockTraces returns all block traces for the given block hash or number. If not found, returns nil.
	GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error)
}
