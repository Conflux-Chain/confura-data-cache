package extract

import (
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

type FinalizedHeightProvider func() (uint64, error)

// BlockHashCache maintains a fixed-size sliding window of block hashes,
// mapping block numbers to hashes. Old entries are evicted automatically
// based on either fixed capacity or finalized block height.
type BlockHashCache struct {
	mu          sync.RWMutex
	capacity    uint
	blockHashes map[uint64]common.Hash
	start, end  uint64
	finalized   atomic.Uint64
	provider    FinalizedHeightProvider
}

// NewBlockHashCache creates a new BlockHashCache with the specified capacity.
func NewBlockHashCache(size uint) *BlockHashCache {
	return &BlockHashCache{
		capacity:    size,
		blockHashes: make(map[uint64]common.Hash),
	}
}

// NewBlockHashCacheWithProvider creates a new BlockHashCache with the specified provider.
func NewBlockHashCacheWithProvider(p FinalizedHeightProvider) *BlockHashCache {
	return &BlockHashCache{
		provider:    p,
		blockHashes: make(map[uint64]common.Hash),
	}
}

// Flush clears all the block hashes in the cache.
func (w *BlockHashCache) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.start, w.end = 0, 0
	w.blockHashes = make(map[uint64]common.Hash)
}

// Len returns the current number of block hashes in the cache.
func (w *BlockHashCache) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.blockHashes)
}

// Append inserts a new block hash into the cache. It enforces:
// - automatic eviction if the cache is full (capacity-based),
// - eviction of finalized blocks (if a provider is configured),
// - continuity of block numbers.
func (w *BlockHashCache) Append(blockNumber uint64, blockHash common.Hash) error {
	if err := w.initializeFinalizedBlockNumber(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Enforce block number continuity (monotonic increase by 1)
	if len(w.blockHashes) > 0 && blockNumber != w.end+1 {
		return errors.Errorf("block number not continuous, expected %v got %v", w.end+1, blockNumber)
	}

	w.purgeFinalized()
	w.evictIfFull()

	// Initialize window range on first insert
	if len(w.blockHashes) == 0 {
		w.start = blockNumber
	}
	w.end = blockNumber
	w.blockHashes[blockNumber] = blockHash
	return nil
}

// purgeFinalized removes all blocks up to the finalized block number,
// if a provider is configured and capacity is 0.
func (w *BlockHashCache) purgeFinalized() {
	if w.capacity == 0 && w.provider != nil && len(w.blockHashes) > 0 {
		finalized := w.finalized.Load()
		for w.start <= min(finalized, w.end) {
			delete(w.blockHashes, w.start)
			w.start++
		}
	}
}

func (w *BlockHashCache) initializeFinalizedBlockNumber() error {
	if w.provider != nil {
		number, err := w.provider()
		if err != nil {
			return errors.WithMessage(err, "failed to get finalized block number")
		}
		w.finalized.Store(number)
	}
	return nil
}

// evictIfFull removes the oldest block hashes until capacity is satisfied.
func (w *BlockHashCache) evictIfFull() {
	if w.capacity == 0 || len(w.blockHashes) == 0 {
		return
	}
	for len(w.blockHashes) >= int(w.capacity) {
		delete(w.blockHashes, w.start)
		w.start++
	}
}

// Latest returns the most recent block number and its corresponding hash.
func (w *BlockHashCache) Latest() (uint64, common.Hash, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if len(w.blockHashes) == 0 {
		return 0, common.Hash{}, false
	}
	return w.end, w.blockHashes[w.end], true
}

// Pop removes and returns the latest block number and hash.
func (w *BlockHashCache) Pop() (uint64, common.Hash, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.blockHashes) == 0 {
		return 0, common.Hash{}, false
	}
	bn, hash := w.end, w.blockHashes[w.end]
	delete(w.blockHashes, w.end)
	w.end--
	return bn, hash, true
}
