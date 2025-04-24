package extract

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/Conflux-Chain/confura-data-cache/types"
)

// Sizable represents types that report their memory footprint.
type Sizable interface {
	Size() uint64
}

// ReorgAwareBlockData wraps a block data with reorg information.
type ReorgAwareBlockData[T Sizable] struct {
	blockData   T
	reorgHeight *uint64
	cachedSize  atomic.Uint64
}

func (r *ReorgAwareBlockData[T]) ReorgHeight() (uint64, bool) {
	if r.reorgHeight != nil {
		return *r.reorgHeight, true
	}
	return 0, false
}

func (r *ReorgAwareBlockData[T]) BlockData() (v T, ok bool) {
	if r.reorgHeight != nil {
		return
	}
	return r.blockData, true
}

func (r *ReorgAwareBlockData[T]) Size() uint64 {
	if r.reorgHeight != nil {
		return 0
	}
	if v := r.cachedSize.Load(); v != 0 {
		return v
	}

	size := r.blockData.Size()
	r.cachedSize.Store(size)
	return size
}

// MemoryBoundedChannel wraps a memory-bounded channel.
type MemoryBoundedChannel[T Sizable] struct {
	mu           sync.RWMutex
	size         uint64     // current memory size used by buffered items
	capacity     uint64     // memory limit in bytes (0 = unlimited)
	buffer       *list.List // buffered items to receive (FIFO)
	notFullCond  *sync.Cond // signals when memory is not full
	notEmptyCond *sync.Cond // signals when buffer is not empty
}

// NewMemoryBoundedChannel creates a new memory-bounded channel.
func NewMemoryBoundedChannel[T Sizable](capacity uint64) *MemoryBoundedChannel[T] {
	if capacity == 0 {
		panic("capacity must be greater than 0")
	}

	m := &MemoryBoundedChannel[T]{
		capacity: capacity,
		buffer:   list.New(),
	}
	m.notFullCond = sync.NewCond(&m.mu)
	m.notEmptyCond = sync.NewCond(&m.mu)
	return m
}

// Send blocks until enough memory is available to buffer the item.
func (m *MemoryBoundedChannel[T]) Send(item T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	size := item.Size()
	for m.capacity > 0 && m.size+size > m.capacity && m.buffer.Len() > 0 {
		m.notFullCond.Wait()
	}
	m.enqueue(item, size)
}

// TrySend attempts to send without blocking. Returns false if over memory limit.
func (m *MemoryBoundedChannel[T]) TrySend(item T) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	size := item.Size()
	if m.capacity > 0 && m.size+size > m.capacity && m.buffer.Len() > 0 {
		return false
	}
	m.enqueue(item, size)
	return true
}

// Receive blocks until an item is available and returns it.
func (m *MemoryBoundedChannel[T]) Receive() T {
	m.mu.Lock()
	defer m.mu.Unlock()

	for m.buffer.Len() == 0 {
		m.notEmptyCond.Wait()
	}
	return m.dequeue()
}

// TryReceive returns an item if available, otherwise false.
func (m *MemoryBoundedChannel[T]) TryReceive() (v T, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buffer.Len() > 0 {
		v, ok = m.dequeue(), true
	}
	return
}

// Len returns the number of items in the channel.
func (m *MemoryBoundedChannel[T]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.buffer.Len()
}

// enqueue adds item and updates memory.
func (m *MemoryBoundedChannel[T]) enqueue(item T, size uint64) {
	m.buffer.PushBack(item)
	m.size += size

	m.notEmptyCond.Broadcast()
}

// dequeue removes and returns front item, updating memory.
func (m *MemoryBoundedChannel[T]) dequeue() T {
	elem := m.buffer.Front()
	m.buffer.Remove(elem)

	item := elem.Value.(T)
	m.size -= item.Size()

	m.notFullCond.Broadcast()
	return item
}

// Convenience alias for Eth ReorgAwareBlockData.
type EthReorgAwareBlockData = ReorgAwareBlockData[*types.EthBlockData]

func NewEthReorgAwareBlockData(blockData *types.EthBlockData) *EthReorgAwareBlockData {
	return &EthReorgAwareBlockData{blockData: blockData}
}

func NewEthReorgAwareBlockDataWithHeight(reorgHeight uint64) *EthReorgAwareBlockData {
	return &EthReorgAwareBlockData{reorgHeight: &reorgHeight}
}

// Convenience alias for EthBlockData channels.
type EthMemoryBoundedChannel = MemoryBoundedChannel[*EthReorgAwareBlockData]

func NewEthMemoryBoundedChannel(capacity uint64) *EthMemoryBoundedChannel {
	return NewMemoryBoundedChannel[*EthReorgAwareBlockData](capacity)
}
