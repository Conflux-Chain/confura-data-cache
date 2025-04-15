package extract

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

// BlockHashWindow caches block hashes in a ring buffer for reorg check.
type BlockHashWindow[T any] struct {
	mu     sync.RWMutex
	next   int    // Index for next write
	count  int    // Current number of elements
	buff   []T    // Ring buffer of block hashes
	latest uint64 // Latest block number
}

// NewBlockHashWindow creates a fixed-size window for block hashes.
func NewBlockHashWindow[T any](size int) *BlockHashWindow[T] {
	if size <= 0 {
		panic("window size must be positive")
	}
	return &BlockHashWindow[T]{
		buff: make([]T, size),
	}
}

// Capacity returns the window capacity.
func (w *BlockHashWindow[T]) Capacity() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return len(w.buff)
}

// Len returns current number of elements in the buffer.
func (w *BlockHashWindow[T]) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.count
}

// Push inserts a new block hash into the ring buffer.
func (w *BlockHashWindow[T]) Push(blockNumber uint64, blockHash T) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count > 0 && blockNumber != w.latest+1 {
		return errors.Errorf("block number not continuous, expected %v got %v", w.latest+1, blockNumber)
	}

	w.latest = blockNumber
	w.buff[w.next] = blockHash

	w.next = (w.next + 1) % len(w.buff)
	if w.count < len(w.buff) {
		w.count++
	}
	return nil
}

// Peek returns the latest block number and hash.
func (w *BlockHashWindow[T]) Peek() (bn uint64, bh T, found bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.count == 0 {
		return
	}
	idx := (w.next - 1 + len(w.buff)) % len(w.buff)
	return w.latest, w.buff[idx], true
}

// Pop removes and returns the latest block hash.
func (w *BlockHashWindow[T]) Pop() (bn uint64, bh T) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count == 0 {
		return
	}

	idx := (w.next - 1 + len(w.buff)) % len(w.buff)
	blockNum, hash := w.latest, w.buff[idx]

	w.next = idx
	w.buff[w.next] = *new(T) // Clear the slot

	w.latest--
	w.count--

	return blockNum, hash
}

// EthBlockHashWindow is a specialized BlockHashWindow for Ethereum block hashes.
type EthBlockHashWindow = BlockHashWindow[common.Hash]

func NewEthBlockHashWindow(size int) *EthBlockHashWindow {
	return (*EthBlockHashWindow)(NewBlockHashWindow[common.Hash](size))
}
