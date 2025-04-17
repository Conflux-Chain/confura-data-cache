package nearhead

import (
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
)

type Cache interface {
	Put(data *types.EthBlockData) error
	Pop(blockNumber uint64) bool
	GetBlockByNumber(blockNumber uint64, isFull bool) *ethTypes.Block
	GetBlockByHash(blockHash common.Hash, isFull bool) *ethTypes.Block
	GetTransactionByHash(txHash common.Hash) *ethTypes.TransactionDetail
	GetBlockReceiptsByHash(blockHash common.Hash) []ethTypes.Receipt
	GetBlockReceiptsByNumber(blockNumber uint64) []ethTypes.Receipt
	GetTransactionReceipt(txHash common.Hash) *ethTypes.Receipt
	GetBlockTracesByHash(blockHash common.Hash) []ethTypes.LocalizedTrace
	GetBlockTracesByNumber(blockNumber uint64) []ethTypes.LocalizedTrace
	GetTransactionTraces(txHash common.Hash) []ethTypes.LocalizedTrace
}
