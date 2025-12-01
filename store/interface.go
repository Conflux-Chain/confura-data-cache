package store

import (
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
)

type Writable interface {
	NextBlockNumber() uint64
	Write(...types.EthBlockData) error
}

type Readable interface {
	// block
	GetBlockNumber(bhon types.BlockHashOrNumber) (uint64, bool, error)
	GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error)
	GetBlockTransactionCount(bhon types.BlockHashOrNumber) (int64, error)

	// transaction
	GetTransactionByHash(txHash common.Hash) (*ethTypes.TransactionDetail, error)
	GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (*ethTypes.TransactionDetail, error)

	// receipt
	GetTransactionReceipt(txHash common.Hash) (*ethTypes.Receipt, error)
	GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error)

	// trace
	GetTrace(txHash common.Hash, index uint) (*ethTypes.LocalizedTrace, error)
	GetTransactionTraces(txHash common.Hash) ([]ethTypes.LocalizedTrace, error)
	GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error)
}

type Store interface {
	Writable
	Readable
}
