package nearhead

import (
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// EthCache is used to cache near head data
type EthCache struct {
	frontBlockNumber       *uint64
	blockNumber2BlockDatas map[uint64]*types.EthBlockData
	blockHash2BlockNumbers map[common.Hash]uint64
	txHash2TxIndexes       map[common.Hash]TransactionIndex
	rwMutex                sync.RWMutex
}

func NewEthCache() *EthCache {
	return &EthCache{
		blockNumber2BlockDatas: make(map[uint64]*types.EthBlockData),
		blockHash2BlockNumbers: make(map[common.Hash]uint64),           // mapping from block hash to number, for query by block hash
		txHash2TxIndexes:       make(map[common.Hash]TransactionIndex), // mapping from tx hash to block number and tx index, for query by tx hash
	}
}

// Put is used to add near head data to memory cache
func (c *EthCache) Put(data *types.EthBlockData) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	// check block number in sequence
	bn := data.Block.Number.Uint64()
	if c.frontBlockNumber != nil {
		next := *c.frontBlockNumber + 1
		if next != bn {
			return errors.Errorf("Block data not cached in sequence, expected = %v, actual = %v", next, bn)
		}
	}

	// TODO evict if exceeds maxsize

	c.blockNumber2BlockDatas[bn] = data
	c.frontBlockNumber = &bn
	c.blockHash2BlockNumbers[data.Block.Hash] = bn
	for i, tx := range data.Block.Transactions.Transactions() {
		c.txHash2TxIndexes[tx.Hash] = TransactionIndex{
			blockNumber:      bn,
			transactionIndex: uint64(i),
		}
	}

	return nil
}

// GetBlockByNumber returns block with given number.
func (c *EthCache) GetBlockByNumber(blockNumber uint64, isFull bool) *ethTypes.Block {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	data, exists := c.blockNumber2BlockDatas[blockNumber]
	if !exists {
		return nil
	}

	if isFull {
		return data.Block
	}

	txs := data.Block.Transactions.Transactions()
	hashes := make([]common.Hash, len(txs))
	for i, tx := range txs {
		hashes[i] = tx.Hash
	}
	txOrHashList := ethTypes.NewTxOrHashListByHashes(hashes)

	block := *data.Block
	block.Transactions = *txOrHashList
	return &block
}

// GetBlockByHash returns block with given block hash.
func (c *EthCache) GetBlockByHash(blockHash common.Hash, isFull bool) *ethTypes.Block {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockByNumber(blockNumber, isFull)
}

// GetTransactionByHash returns transaction with given transaction hash.
func (c *EthCache) GetTransactionByHash(txHash common.Hash) *ethTypes.TransactionDetail {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	txIndex, exists := c.txHash2TxIndexes[txHash]
	if !exists {
		return nil
	}

	data, exists := c.blockNumber2BlockDatas[txIndex.blockNumber]
	if !exists {
		return nil
	}

	tx := data.Block.Transactions.Transactions()[txIndex.transactionIndex]
	return &tx
}

// GetBlockReceiptsByHash returns the receipts of a given block hash.
func (c *EthCache) GetBlockReceiptsByHash(blockHash common.Hash) []*ethTypes.Receipt {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockReceiptsByNumber(blockNumber)
}

// GetBlockReceiptsByNumber returns the receipts of a given block number.
func (c *EthCache) GetBlockReceiptsByNumber(blockNumber uint64) []*ethTypes.Receipt {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	data, exists := c.blockNumber2BlockDatas[blockNumber]
	if !exists {
		return nil
	}

	return data.Receipts
}

// GetTransactionReceipt returns transaction receipt by transaction hash.
func (c *EthCache) GetTransactionReceipt(txHash common.Hash) *ethTypes.Receipt {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	txIndex, exists := c.txHash2TxIndexes[txHash]
	if !exists {
		return nil
	}

	data, exists := c.blockNumber2BlockDatas[txIndex.blockNumber]
	if !exists {
		return nil
	}

	receipt := data.Receipts[txIndex.transactionIndex]
	return receipt
}

// GetBlockTracesByHash returns all traces produced at given block by hash
func (c *EthCache) GetBlockTracesByHash(blockHash common.Hash) []ethTypes.LocalizedTrace {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockTracesByNumber(blockNumber)
}

// GetBlockTracesByNumber returns all traces produced at given block by number
func (c *EthCache) GetBlockTracesByNumber(blockNumber uint64) []ethTypes.LocalizedTrace {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	data, exists := c.blockNumber2BlockDatas[blockNumber]
	if !exists {
		return nil
	}

	return data.Traces
}

// GetTransactionTraces returns all traces of given transaction.
func (c *EthCache) GetTransactionTraces(txHash common.Hash) []ethTypes.LocalizedTrace {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	txIndex, exists := c.txHash2TxIndexes[txHash]
	if !exists {
		return nil
	}

	data, exists := c.blockNumber2BlockDatas[txIndex.blockNumber]
	if !exists {
		return nil
	}

	traces := make([]ethTypes.LocalizedTrace, 0)
	for _, trace := range data.Traces {
		if txHash == *trace.TransactionHash {
			traces = append(traces, trace)
		}
	}

	return traces
}

type TransactionIndex struct {
	blockNumber      uint64
	transactionIndex uint64
}
