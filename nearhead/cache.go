package nearhead

import (
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// Config is cache configurations
type Config struct {
	MaxMemory uint64 `default:"104857600"` // 100MB
	MaxLogs   int    `default:"10000"`
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
func (c *EthCache) Put(sized *types.Sized[*types.EthBlockData]) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	// check block number in sequence
	data := sized.Value
	bn := data.Block.Number.Uint64()
	if c.end > c.start && c.end != bn {
		return errors.Errorf("Block data not cached in sequence, expected = %v, actual = %v", c.end, bn)
	}

	// check if exceeds max memory
	dataSize := uint64(sized.Size)
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

// GetBlock returns block with given number or hash.
func (c *EthCache) GetBlock(bhon types.BlockHashOrNumber, isFull bool) *ethTypes.Block {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber := c.getBlockNumber(bhon)
	if blockNumber == nil {
		return nil
	}

	data, exists := c.blockNumber2BlockDatas[*blockNumber]
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

// GetBlockReceipts returns the receipts of a given block number or hash.
func (c *EthCache) GetBlockReceipts(bhon types.BlockHashOrNumber) []*ethTypes.Receipt {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber := c.getBlockNumber(bhon)
	if blockNumber == nil {
		return nil
	}

	data, exists := c.blockNumber2BlockDatas[*blockNumber]
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

	return data.Receipts[txIndex.transactionIndex]
}

// GetBlockTraces returns all traces produced at given block by number or hash
func (c *EthCache) GetBlockTraces(bhon types.BlockHashOrNumber) []ethTypes.LocalizedTrace {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber := c.getBlockNumber(bhon)
	if blockNumber == nil {
		return nil
	}

	data, exists := c.blockNumber2BlockDatas[*blockNumber]
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

// GetLogsByBlockRange returns logs matching given filter parameters.
// nil returned when not cached.
// empty logs returned when not exists.
func (c *EthCache) GetLogsByBlockRange(fromBlock, toBlock uint64, logFilter FilterOpt) (*EthLogs, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	if fromBlock > toBlock {
		return nil, errors.New("invalid block range params")
	}

	// nil returned when not cached
	if c.start == c.end ||
		toBlock < c.start ||
		fromBlock >= c.end {
		return nil, nil
	}

	// contract address filter
	addressMap := make(map[common.Address]bool)
	for _, address := range logFilter.Addresses {
		addressMap[address] = true
	}

	// topic filter
	topicMap := make([]map[common.Hash]bool, 0, 4)
	for i, topics := range logFilter.Topics {
		topicMap = append(topicMap, make(map[common.Hash]bool))
		for _, topic := range topics {
			topicMap[i][topic] = true
		}
	}

	// from / to block filter
	from := max(fromBlock, c.start)
	to := min(toBlock, c.end-1)

	logs := make([]ethTypes.Log, 0)
	for bn := from; bn <= to; bn++ {
		blockLogs := c.collectBlockLogs(bn, addressMap, topicMap)
		logs = append(logs, blockLogs...)
		if len(logs) > c.config.MaxLogs {
			return nil, errors.Errorf("the result set exceeds the max limit of %v logs, please narrow down your filter conditions", c.config.MaxLogs)
		}
	}

	return &EthLogs{
		FromBlock: from,
		ToBlock:   to,
		Logs:      logs,
	}, nil
}

// GetLogsByBlockHash returns logs matching given filter parameters.
// nil returned when not cached.
// empty logs returned when not exists.
func (c *EthCache) GetLogsByBlockHash(blockHash common.Hash, logFilter FilterOpt) (*EthLogs, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil, nil
	}

	return c.GetLogsByBlockRange(blockNumber, blockNumber, logFilter)
}

func (c *EthCache) collectBlockLogs(blockNumber uint64, addrMap map[common.Address]bool,
	topicMap []map[common.Hash]bool) []ethTypes.Log {
	logs := make([]ethTypes.Log, 0)

	for _, receipt := range c.blockNumber2BlockDatas[blockNumber].Receipts {
		for _, log := range receipt.Logs {
			if len(addrMap) > 0 && !addrMap[log.Address] {
				continue
			}

			wanted := true
			for i, t := range log.Topics {
				if len(topicMap) > i && len(topicMap[i]) > 0 && !topicMap[i][t] {
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

// getBlockNumber returns block number for the given block hash or number if any.
func (c *EthCache) getBlockNumber(bhon types.BlockHashOrNumber) *uint64 {
	hash, ok, number := bhon.HashOrNumber()
	if !ok {
		return &number
	}

	blockNumber, exists := c.blockHash2BlockNumbers[hash]
	if !exists {
		return nil
	}

	return &blockNumber
}

type TransactionIndex struct {
	blockNumber      uint64
	transactionIndex uint64
}

// FilterOpt contains options for contract log filtering.
type FilterOpt struct {
	Addresses []common.Address
	Topics    [][]common.Hash
}

type EthLogs struct {
	FromBlock uint64
	ToBlock   uint64
	Logs      []ethTypes.Log
}
