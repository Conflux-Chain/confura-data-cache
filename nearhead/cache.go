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
	blockNumber2BlockDatas map[uint64]*cdcTypes.EthBlockData
	blockNumbers           list.List
	blockHash2BlockNumbers map[common.Hash]uint64
	txHash2TxIndexes       map[common.Hash]TransactionIndex

	rwMutex sync.RWMutex
}

func MustNewEthCache() *EthCache {
	return &EthCache{
		blockNumber2BlockDatas: make(map[uint64]*cdcTypes.EthBlockData),
		blockNumbers:           *list.New(),                            // for evict from list front
		blockHash2BlockNumbers: make(map[common.Hash]uint64),           // mapping from block hash to number, for query by block hash
		txHash2TxIndexes:       make(map[common.Hash]TransactionIndex), // mapping from tx hash to block number and tx index, for query by tx hash
	}
}

// Put is used to add near head data to memory cache
func (c *EthCache) Put(data *cdcTypes.EthBlockData) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	// check block number in sequence
	bn := data.Block.Number.Uint64()
	if c.blockNumbers.Back() != nil {
		next := c.blockNumbers.Back().Value.(uint64) + 1
		if next != bn {
			return errors.Errorf("Block data not cached in sequence, expected = %v, actual = %v", next, bn)
		}
	}

	// TODO evict if exceeds maxsize

	c.blockNumber2BlockDatas[bn] = data
	c.blockNumbers.PushFront(bn)
	c.blockHash2BlockNumbers[data.Block.Hash] = bn
	for _, tx := range data.Block.Transactions.Transactions() {
		c.txHash2TxIndexes[tx.Hash] = TransactionIndex{
			blockNumber:      bn,
			transactionIndex: *tx.TransactionIndex,
		}
	}

	return nil
}

// GetBlockByNumber returns block with given number.
func (c *EthCache) GetBlockByNumber(blockNumber uint64, isFull bool) *types.Block {
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
	txOrHashList := types.NewTxOrHashListByHashes(hashes)

	block := *data.Block
	block.Transactions = *txOrHashList
	return &block
}

// GetBlockByHash returns block with given block hash.
func (c *EthCache) GetBlockByHash(blockHash common.Hash, isFull bool) *types.Block {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	blockNumber, exists := c.blockHash2BlockNumbers[blockHash]
	if !exists {
		return nil
	}

	return c.GetBlockByNumber(blockNumber, isFull)
}

// GetTransactionByHash returns transaction with given transaction hash.
func (c *EthCache) GetTransactionByHash(txHash common.Hash) *types.TransactionDetail {
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
func (c *EthCache) GetBlockReceipts(blockNumOrHash types.BlockNumberOrHash) ([]*types.Receipt, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	if blockNumOrHash.BlockNumber == nil && blockNumOrHash.BlockHash == nil {
		return nil, errors.New("No block number or block hash provided")
	}

	if blockNumOrHash.BlockNumber != nil {
		data, exists := c.blockNumber2BlockDatas[uint64(blockNumOrHash.BlockNumber.Int64())]
		if !exists {
			return nil, nil
		}
		return data.Receipts, nil
	}

	blockNumber, exists := c.blockHash2BlockNumbers[*blockNumOrHash.BlockHash]
	if !exists {
		return nil, nil
	}
	data, exists := c.blockNumber2BlockDatas[blockNumber]
	if !exists {
		return nil, nil
	}
	return data.Receipts, nil
}

// GetTransactionReceipt returns transaction receipt by transaction hash.
func (c *EthCache) GetTransactionReceipt(txHash common.Hash) *types.Receipt {
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

// GetBlockTraces returns all traces produced at given block.
func (c *EthCache) GetBlockTraces(blockNumOrHash types.BlockNumberOrHash) ([]types.LocalizedTrace, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	if blockNumOrHash.BlockNumber == nil && blockNumOrHash.BlockHash == nil {
		return nil, errors.New("No block number or block hash provided")
	}

	if blockNumOrHash.BlockNumber != nil {
		data, exists := c.blockNumber2BlockDatas[uint64(blockNumOrHash.BlockNumber.Int64())]
		if !exists {
			return nil, nil
		}
		return data.Traces, nil
	}

	blockNumber, exists := c.blockHash2BlockNumbers[*blockNumOrHash.BlockHash]
	if !exists {
		return nil, nil
	}
	data, exists := c.blockNumber2BlockDatas[blockNumber]
	if !exists {
		return nil, nil
	}
	return data.Traces, nil
}

// GetTransactionTraces returns all traces of given transaction.
func (c *EthCache) GetTransactionTraces(txHash common.Hash) []types.LocalizedTrace {
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

	traces := make([]types.LocalizedTrace, 0)
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
