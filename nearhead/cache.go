package nearhead

import (
	"container/list"
	"sync"

	cdcTypes "github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// EthCache is used to cache near head data
type EthCache struct {
	blockNumber2Blocks map[uint64]*cdcTypes.EthBlockData

	blockNumbers list.List
	blockHashes  map[common.Hash]uint64
	transactions map[common.Hash]Transaction

	mutex sync.Mutex
}

func MustNewEthCache() *EthCache {
	return &EthCache{
		blockNumber2Blocks: make(map[uint64]*cdcTypes.EthBlockData),
		blockNumbers:       *list.New(),                       // for evict from list front
		blockHashes:        make(map[common.Hash]uint64),      // mapping from block hash to number, for query by block hash
		transactions:       make(map[common.Hash]Transaction), // mapping from tx hash to block number and tx index, for query by tx hash
	}
}

// Put is used to add near head data to memory cache
func (c *EthCache) Put(data *cdcTypes.EthBlockData) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	nextBlockNumber := uint64(0)
	lastBlockNumber := c.blockNumbers.Front()
	if lastBlockNumber != nil {
		nextBlockNumber = lastBlockNumber.Value.(uint64) + 1
	}
	if nextBlockNumber != data.Block.Number.Uint64() {
		return errors.Errorf("Block data not cached in sequence, expected = %v, actual = %v", nextBlockNumber, data.Block.Number.Uint64())
	}

	// TODO evict if exceeds maxsize

	bn := data.Block.Number.Uint64()
	c.blockNumber2Blocks[bn] = data
	c.blockNumbers.PushFront(bn)
	c.blockHashes[data.Block.Hash] = bn
	for _, tx := range data.Block.Transactions.Transactions() {
		c.transactions[tx.Hash] = Transaction{
			blockNumber:      bn,
			transactionIndex: *tx.TransactionIndex,
		}
	}

	return nil
}

// GetBlockByNumber returns block with given number.
func (c *EthCache) GetBlockByNumber(blockNumber uint64, isFull bool) (*types.Block, bool) {
	data, exists := c.blockNumber2Blocks[blockNumber]
	if !exists {
		return nil, false
	}

	if !isFull {
		hashes := make([]common.Hash, 0)
		for _, tx := range data.Block.Transactions.Transactions() {
			hashes = append(hashes, tx.Hash)
		}
		txOrHashList := types.NewTxOrHashListByHashes(hashes)

		blockClone := *data.Block
		blockClone.Transactions = *txOrHashList
		return &blockClone, exists
	}

	return data.Block, exists
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

	data, exists := c.blockNumber2Blocks[txCache.blockNumber]
	if !exists {
		return nil, false
	}

	tx := data.Block.Transactions.Transactions()[txCache.transactionIndex]
	return &tx, true
}

// GetBlockReceipts returns the receipts of a given block number or hash.
func (c *EthCache) GetBlockReceipts(blockNumOrHash types.BlockNumberOrHash) ([]*types.Receipt, bool, error) {
	if blockNumOrHash.BlockNumber == nil && blockNumOrHash.BlockHash == nil {
		return nil, false, errors.New("No block number or block hash provided")
	}

	if blockNumOrHash.BlockNumber != nil {
		data, exists := c.blockNumber2Blocks[uint64(blockNumOrHash.BlockNumber.Int64())]
		return data.Receipts, exists, nil
	}

	blockNumber, exists := c.blockHashes[*blockNumOrHash.BlockHash]
	if !exists {
		return nil, false, nil
	}

	data, exists := c.blockNumber2Blocks[blockNumber]
	return data.Receipts, exists, nil
}

// GetTransactionReceipt returns transaction receipt by transaction hash.
func (c *EthCache) GetTransactionReceipt(txHash common.Hash) (*types.Receipt, bool) {
	txCache, exists := c.transactions[txHash]
	if !exists {
		return nil, false
	}

	data, exists := c.blockNumber2Blocks[txCache.blockNumber]
	if !exists {
		return nil, false
	}

	receipt := data.Receipts[txCache.transactionIndex]
	return receipt, true
}

// GetBlockTraces returns all traces produced at given block.
func (c *EthCache) GetBlockTraces(blockNumOrHash types.BlockNumberOrHash) ([]types.LocalizedTrace, bool, error) {
	if blockNumOrHash.BlockNumber == nil && blockNumOrHash.BlockHash == nil {
		return nil, false, errors.New("No block number or block hash provided")
	}

	if blockNumOrHash.BlockNumber != nil {
		data, exists := c.blockNumber2Blocks[uint64(blockNumOrHash.BlockNumber.Int64())]
		return data.Traces, exists, nil
	}

	blockNumber, exists := c.blockHashes[*blockNumOrHash.BlockHash]
	if !exists {
		return nil, false, nil
	}

	data, exists := c.blockNumber2Blocks[blockNumber]
	return data.Traces, exists, nil
}

// GetTransactionTraces returns all traces of given transaction.
func (c *EthCache) GetTransactionTraces(txHash common.Hash) ([]types.LocalizedTrace, bool) {
	txCache, exists := c.transactions[txHash]
	if !exists {
		return nil, false
	}

	data, exists := c.blockNumber2Blocks[txCache.blockNumber]

	txTraces := make([]types.LocalizedTrace, 0)
	for _, trace := range data.Traces {
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
