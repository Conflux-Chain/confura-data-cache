package nearhead

import (
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/DmitriyVTitov/size"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// Config is cache configurations
type Config struct {
	MaxMemory uint64 `default:"104857600"` // 100MG
}

// EthCache is used to cache near head data
type EthCache struct {
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

func NewEthCache(config Config) *EthCache {
	return &EthCache{
		config:                 config,
		blockNumber2BlockDatas: make(map[uint64]*types.EthBlockData),
		blockHash2BlockNumbers: make(map[common.Hash]uint64),           // mapping from block hash to number, for query by block hash
		txHash2TxIndexes:       make(map[common.Hash]TransactionIndex), // mapping from tx hash to block number and tx index, for query by tx hash
		blockNumber2BlockSize:  make(map[uint64]uint64),                // mapping from block number to block data size
	}
}

// Put is used to add near head data to memory cache
func (c *EthCache) Put(data *types.EthBlockData) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	// check block number in sequence
	bn := data.Block.Number.Uint64()
	if c.end > 0 && c.end != bn {
		return errors.Errorf("Block data not cached in sequence, expected = %v, actual = %v", c.end, bn)
	}

	// check if exceeds max memory
	dataSize := uint64(size.Of(data))
	if dataSize > c.config.MaxMemory {
		return errors.Errorf("Block data exceeds max memory, bn = %v, size = %v, max = %v", bn, dataSize, c.config.MaxMemory)
	}
	for c.currentSize+dataSize > c.config.MaxMemory {
		c.pop()
	}

	// push data
	if c.start == uint64(0) {
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

// pop always remove the earliest block data.
func (c *EthCache) pop() {
	if c.start >= c.end {
		c.resetPosition()
		return
	}

	bn := c.start
	data, exists := c.blockNumber2BlockDatas[bn]
	if !exists {
		return
	}

	// pop data
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

	c.start += 1
	if c.start >= c.end {
		c.resetPosition()
	}
}

func (c *EthCache) resetPosition() {
	c.start = uint64(0)
	c.end = uint64(0)
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
func (c *EthCache) GetBlockReceiptsByHash(blockHash common.Hash) []ethTypes.Receipt {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockReceiptsByNumber(blockNumber)
}

// GetBlockReceiptsByNumber returns the receipts of a given block number.
func (c *EthCache) GetBlockReceiptsByNumber(blockNumber uint64) []ethTypes.Receipt {
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
	return &receipt
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
