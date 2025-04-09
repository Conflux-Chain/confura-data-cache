package cache

import (
	"container/list"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// EthCache is used to cache near head data
type EthCache struct {
	blocks   map[uint64]*types.Block
	receipts map[uint64][]*types.Receipt
	traces   map[uint64][]types.LocalizedTrace

	blockNumbers list.List
	blockHashes  map[common.Hash]uint64
	transactions map[common.Hash]Transaction

	mutex sync.Mutex
}

func MustNewEthCache() *EthCache {
	return &EthCache{
		blocks:   make(map[uint64]*types.Block),
		receipts: make(map[uint64][]*types.Receipt),
		traces:   make(map[uint64][]types.LocalizedTrace),

		blockNumbers: *list.New(),                       // for evict from list front
		blockHashes:  make(map[common.Hash]uint64),      // mapping from block hash to number, for query by block hash
		transactions: make(map[common.Hash]Transaction), // mapping from tx hash to block number and tx index, for query by tx hash
	}
}

// Set is used to add near head data to memory cache
func (c *EthCache) Set(block *types.Block, receipts []*types.Receipt, traces []types.LocalizedTrace) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// TODO evict if exceeds maxsize

	bn := block.Number.Uint64()
	c.blocks[bn] = block
	c.receipts[bn] = receipts
	c.traces[bn] = traces

	c.blockNumbers.PushFront(bn)
	c.blockHashes[block.Hash] = bn
	for _, tx := range block.Transactions.Transactions() {
		c.transactions[tx.Hash] = Transaction{
			blockNumber:      bn,
			transactionIndex: *tx.TransactionIndex,
		}
	}

	return nil
}

// GetBlockByNumber returns block with given number.
func (c *EthCache) GetBlockByNumber(blockNumber uint64, isFull bool) (*types.Block, bool) {
	block, exists := c.blocks[blockNumber]
	if !exists {
		return nil, false
	}

	if !isFull {
		hashes := make([]common.Hash, 0)
		for _, tx := range block.Transactions.Transactions() {
			hashes = append(hashes, tx.Hash)
		}
		txOrHashList := types.NewTxOrHashListByHashes(hashes)

		blockClone := *block
		blockClone.Transactions = *txOrHashList
		return &blockClone, exists
	}

	return block, exists
}

// GetBlockByHash returns block with given block hash.
func (c *EthCache) GetBlockByHash(blockHash common.Hash, isFull bool) (*types.Block, bool) {
	blockNumber, exists := c.blockHashes[blockHash]
	if !exists {
		return nil, false
	}

	return c.GetBlockByNumber(blockNumber, isFull)
}

// GetTransactionByHash returns transaction with given transaction hash.
func (c *EthCache) GetTransactionByHash(txHash common.Hash) (*types.TransactionDetail, bool) {
	txCache, exists := c.transactions[txHash]
	if !exists {
		return nil, false
	}

	block, exists := c.blocks[txCache.blockNumber]
	if !exists {
		return nil, false
	}

	tx := block.Transactions.Transactions()[txCache.transactionIndex]
	return &tx, true
}

// GetBlockReceipts returns the receipts of a given block number or hash.
func (c *EthCache) GetBlockReceipts(blockNumOrHash types.BlockNumberOrHash) ([]*types.Receipt, bool, error) {
	if blockNumOrHash.BlockNumber == nil && blockNumOrHash.BlockHash == nil {
		return nil, false, errors.New("No block number or block hash provided")
	}

	if blockNumOrHash.BlockNumber != nil {
		receipts, exists := c.receipts[uint64(blockNumOrHash.BlockNumber.Int64())]
		return receipts, exists, nil
	}

	blockNumber, exists := c.blockHashes[*blockNumOrHash.BlockHash]
	if !exists {
		return nil, false, nil
	}

	receipts, exists := c.receipts[blockNumber]
	return receipts, exists, nil
}

// GetTransactionReceipt returns transaction receipt by transaction hash.
func (c *EthCache) GetTransactionReceipt(txHash common.Hash) (*types.Receipt, bool) {
	txCache, exists := c.transactions[txHash]
	if !exists {
		return nil, false
	}

	receipts, exists := c.receipts[txCache.blockNumber]
	if !exists {
		return nil, false
	}

	receipt := receipts[txCache.transactionIndex]
	return receipt, true
}

// GetBlockTraces returns all traces produced at given block.
func (c *EthCache) GetBlockTraces(blockNumOrHash types.BlockNumberOrHash) ([]types.LocalizedTrace, bool, error) {
	if blockNumOrHash.BlockNumber == nil && blockNumOrHash.BlockHash == nil {
		return nil, false, errors.New("No block number or block hash provided")
	}

	if blockNumOrHash.BlockNumber != nil {
		receipts, exists := c.traces[uint64(blockNumOrHash.BlockNumber.Int64())]
		return receipts, exists, nil
	}

	blockNumber, exists := c.blockHashes[*blockNumOrHash.BlockHash]
	if !exists {
		return nil, false, nil
	}

	traces, exists := c.traces[blockNumber]
	return traces, exists, nil
}

// GetTransactionTraces returns all traces of given transaction.
func (c *EthCache) GetTransactionTraces(txHash common.Hash) ([]types.LocalizedTrace, bool) {
	txCache, exists := c.transactions[txHash]
	if !exists {
		return nil, false
	}

	traces, exists := c.traces[txCache.blockNumber]

	txTraces := make([]types.LocalizedTrace, 0)
	for _, trace := range traces {
		if txHash == *trace.TransactionHash {
			txTraces = append(txTraces, trace)
		}
	}

	return txTraces, exists
}

type Transaction struct {
	blockNumber      uint64
	transactionIndex uint64
}
