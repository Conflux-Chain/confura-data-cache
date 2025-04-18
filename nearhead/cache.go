package nearhead

import (
	"sync"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// Config is cache configurations
type Config struct {
	MaxMemory uint64 `default:"104857600"` // 100MB
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
func (c *EthCache) Pop(blockNumber uint64) bool {
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
func (c *EthCache) evict() {
	bn := c.start
	c.del(bn)
	c.start += 1
}

func (c *EthCache) del(bn uint64) {
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

// GetLogs returns logs matching given filter object.
func (c *EthCache) GetLogs(logFilter FilterQuery) []ethTypes.Log {
	if c.start == c.end {
		return nil
	}

	// if blockHash is present in the filter criteria, neither fromBlock nor toBlock is allowed
	if logFilter.BlockHash != nil && (logFilter.FromBlock != nil || logFilter.ToBlock != nil) {
		return nil
	}

	// contract address filter
	addresses := mapset.NewSet(logFilter.Addresses...)

	// topic filter
	topics := make([]mapset.Set[common.Hash], 0, 4)
	for _, t := range logFilter.Topics {
		topics = append(topics, mapset.NewSet(t...))
	}

	// block hash filter
	if logFilter.BlockHash != nil {
		bn, exist := c.blockHash2BlockNumbers[*logFilter.BlockHash]
		if !exist {
			return nil
		}
		return c.collectBlockLogs(bn, addresses, topics)
	}

	// from / to block filter
	from := logFilter.FromBlock
	to := logFilter.ToBlock
	if from == nil {
		from = &c.end
	}
	if to == nil {
		to = &c.end
	}
	if (*from < c.start || *from >= c.end) ||
		(*to < c.start || *to >= c.end) ||
		(*from > *to) {
		return nil
	}

	logs := make([]ethTypes.Log, 0)
	for bn := *from; bn <= *to; bn++ {
		blockLogs := c.collectBlockLogs(bn, addresses, topics)
		logs = append(logs, blockLogs...)
	}

	return logs
}

func (c *EthCache) collectBlockLogs(blockNumber uint64, addrSet mapset.Set[common.Address],
	topicSet []mapset.Set[common.Hash]) []ethTypes.Log {
	logs := make([]ethTypes.Log, 0)

	for _, receipt := range c.blockNumber2BlockDatas[blockNumber].Receipts {
		for _, log := range receipt.Logs {
			if !addrSet.IsEmpty() && !addrSet.Contains(log.Address) {
				continue
			}

			wanted := true
			for i, t := range log.Topics {
				if len(topicSet) > i && !topicSet[i].IsEmpty() && !topicSet[i].Contains(t) {
					wanted = false
					break
				}
			}

			if wanted {
				logs = append(logs, *log)
			}
		}
	}

	return logs
}

type TransactionIndex struct {
	blockNumber      uint64
	transactionIndex uint64
}

// FilterQuery contains options for contract log filtering.
type FilterQuery struct {
	BlockHash *common.Hash
	FromBlock *uint64
	ToBlock   *uint64
	Addresses []common.Address
	Topics    [][]common.Hash
}
