package nearhead

import (
	"sync"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// Config is cache configurations
type Config struct {
	MaxMemory uint64 `default:"104857600"` // 100MB
}

type EthCache struct {
	cache   *ethCache
	metrics Metrics
}

func NewEthCache(config Config) *EthCache {
	return &EthCache{
		cache: &ethCache{
			config:                 config,
			blockNumber2BlockDatas: make(map[uint64]*types.EthBlockData),
			blockHash2BlockNumbers: make(map[common.Hash]uint64),           // mapping from block hash to number, for query by block hash
			txHash2TxIndexes:       make(map[common.Hash]TransactionIndex), // mapping from tx hash to block number and tx index, for query by tx hash
			blockNumber2BlockSize:  make(map[uint64]uint64),                // mapping from block number to block data size
		},
	}
}

func (c *EthCache) Put(data *types.EthBlockData) error {
	start := time.Now()
	if err := c.cache.Put(data); err != nil {
		return err
	}

	c.metrics.Latest().Update(data.Block.Number.Int64())
	c.metrics.CurrentSize().Update(int64(c.cache.currentSize))
	c.metrics.Put().UpdateSince(start)

	return nil
}

func (c *EthCache) Pop(blockNumber uint64) bool {
	return c.cache.Pop(blockNumber)
}

func (c *EthCache) GetBlockByNumber(blockNumber uint64, isFull bool) *ethTypes.Block {
	block := c.cache.GetBlockByNumber(blockNumber, isFull)
	c.metrics.Hit("getBlockByNumber").Mark(block != nil)
	return block
}

// GetBlockByHash returns block with given block hash.
func (c *EthCache) GetBlockByHash(blockHash common.Hash, isFull bool) *ethTypes.Block {
	block := c.cache.GetBlockByHash(blockHash, isFull)
	c.metrics.Hit("getBlockByHash").Mark(block != nil)
	return block
}

// GetTransactionByHash returns transaction with given transaction hash.
func (c *EthCache) GetTransactionByHash(txHash common.Hash) *ethTypes.TransactionDetail {
	tx := c.cache.GetTransactionByHash(txHash)
	c.metrics.Hit("getTransactionByHash").Mark(tx != nil)
	return tx
}

// GetBlockReceiptsByHash returns the receipts of a given block hash.
func (c *EthCache) GetBlockReceiptsByHash(blockHash common.Hash) []ethTypes.Receipt {
	receipts := c.cache.GetBlockReceiptsByHash(blockHash)
	c.metrics.Hit("getBlockReceiptsByHash").Mark(receipts != nil)
	return receipts
}

// GetBlockReceiptsByNumber returns the receipts of a given block number.
func (c *EthCache) GetBlockReceiptsByNumber(blockNumber uint64) []ethTypes.Receipt {
	receipts := c.cache.GetBlockReceiptsByNumber(blockNumber)
	c.metrics.Hit("getBlockReceiptsByNumber").Mark(receipts != nil)
	return receipts
}

// GetTransactionReceipt returns transaction receipt by transaction hash.
func (c *EthCache) GetTransactionReceipt(txHash common.Hash) *ethTypes.Receipt {
	receipt := c.cache.GetTransactionReceipt(txHash)
	c.metrics.Hit("getTransactionReceipt").Mark(receipt != nil)
	return receipt
}

// GetBlockTracesByHash returns all traces produced at given block by hash
func (c *EthCache) GetBlockTracesByHash(blockHash common.Hash) []ethTypes.LocalizedTrace {
	traces := c.cache.GetBlockTracesByHash(blockHash)
	c.metrics.Hit("GetBlockTracesByHash").Mark(traces != nil)
	return traces
}

// GetBlockTracesByNumber returns all traces produced at given block by number
func (c *EthCache) GetBlockTracesByNumber(blockNumber uint64) []ethTypes.LocalizedTrace {
	traces := c.cache.GetBlockTracesByNumber(blockNumber)
	c.metrics.Hit("getBlockTracesByNumber").Mark(traces != nil)
	return traces
}

// GetTransactionTraces returns all traces of given transaction.
func (c *EthCache) GetTransactionTraces(txHash common.Hash) []ethTypes.LocalizedTrace {
	traces := c.cache.GetTransactionTraces(txHash)
	c.metrics.Hit("getTransactionTraces").Mark(traces != nil)
	return traces
}

// EthCache is used to cache near head data
type ethCache struct {
	config  Config
	rwMutex sync.RWMutex

	start                  uint64 // the first block number, inclusive
	end                    uint64 // the last block number, exclusive
	blockNumber2BlockDatas map[uint64]*types.EthBlockData
	blockHash2BlockNumbers map[common.Hash]uint64
	txHash2TxIndexes       map[common.Hash]TransactionIndex

	currentSize           uint64 // in bytes
	blockNumber2BlockSize map[uint64]uint64
}

// Put is used to add near head data to memory cache
func (c *ethCache) Put(data *types.EthBlockData) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	// check block number in sequence
	bn := data.Block.Number.Uint64()
	if c.end > c.start && c.end != bn {
		return errors.Errorf("Block data not cached in sequence, expected = %v, actual = %v", c.end, bn)
	}

	// check if exceeds max memory
	dataSize := data.Size()
	for c.end-c.start > 0 && c.currentSize+dataSize > c.config.MaxMemory {
		c.evict()
	}

	// push data
	if c.start == c.end {
		c.start = bn
	}
	c.end = bn + uint64(1)
	c.blockNumber2BlockDatas[bn] = data
	c.blockHash2BlockNumbers[data.Block.Hash] = bn
	for i, tx := range data.Block.Transactions.Transactions() {
		c.txHash2TxIndexes[tx.Hash] = TransactionIndex{
			blockNumber:      bn,
			transactionIndex: uint64(i),
		}
	}

	// count data size
	c.blockNumber2BlockSize[bn] = dataSize
	c.currentSize += dataSize

	return nil
}

// Pop clears all blockdata after block number(includes)
// If pop succeeds, returns true, otherwise, returns false
func (c *ethCache) Pop(blockNumber uint64) bool {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.start == c.end || blockNumber < c.start || blockNumber >= c.end {
		return false
	}

	for bn := c.end - 1; bn >= blockNumber; bn-- {
		c.del(bn)
	}
	c.end = blockNumber

	return true
}

// evict always remove the earliest block data.
func (c *ethCache) evict() {
	bn := c.start
	c.del(bn)
	c.start += 1
}

func (c *ethCache) del(bn uint64) {
	data, exists := c.blockNumber2BlockDatas[bn]
	if !exists {
		return
	}

	// evict data
	delete(c.blockNumber2BlockDatas, bn)
	delete(c.blockHash2BlockNumbers, data.Block.Hash)
	txs := data.Block.Transactions.Transactions()
	for _, tx := range txs {
		delete(c.txHash2TxIndexes, tx.Hash)
	}

	// subtract data size
	dataSize := c.blockNumber2BlockSize[bn]
	c.currentSize = c.currentSize - dataSize
	delete(c.blockNumber2BlockSize, bn)
}

// GetBlockByNumber returns block with given number.
func (c *ethCache) GetBlockByNumber(blockNumber uint64, isFull bool) *ethTypes.Block {
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
func (c *ethCache) GetBlockByHash(blockHash common.Hash, isFull bool) *ethTypes.Block {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockByNumber(blockNumber, isFull)
}

// GetTransactionByHash returns transaction with given transaction hash.
func (c *ethCache) GetTransactionByHash(txHash common.Hash) *ethTypes.TransactionDetail {
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
func (c *ethCache) GetBlockReceiptsByHash(blockHash common.Hash) []ethTypes.Receipt {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockReceiptsByNumber(blockNumber)
}

// GetBlockReceiptsByNumber returns the receipts of a given block number.
func (c *ethCache) GetBlockReceiptsByNumber(blockNumber uint64) []ethTypes.Receipt {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	data, exists := c.blockNumber2BlockDatas[blockNumber]
	if !exists {
		return nil
	}

	return data.Receipts
}

// GetTransactionReceipt returns transaction receipt by transaction hash.
func (c *ethCache) GetTransactionReceipt(txHash common.Hash) *ethTypes.Receipt {
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
	return &receipt
}

// GetBlockTracesByHash returns all traces produced at given block by hash
func (c *ethCache) GetBlockTracesByHash(blockHash common.Hash) []ethTypes.LocalizedTrace {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockTracesByNumber(blockNumber)
}

// GetBlockTracesByNumber returns all traces produced at given block by number
func (c *ethCache) GetBlockTracesByNumber(blockNumber uint64) []ethTypes.LocalizedTrace {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	data, exists := c.blockNumber2BlockDatas[blockNumber]
	if !exists {
		return nil
	}

	return data.Traces
}

// GetTransactionTraces returns all traces of given transaction.
func (c *ethCache) GetTransactionTraces(txHash common.Hash) []ethTypes.LocalizedTrace {
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
