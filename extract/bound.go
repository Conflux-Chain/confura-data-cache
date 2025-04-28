package extract

import (
	"container/list"
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/types"
)

// RevertableBlockData wraps a block data with optional reorg information.
type RevertableBlockData[T any] struct {
	BlockData   T       // block data
	ReorgHeight *uint64 // reorg height
}

// MemoryBoundedChannel wraps a memory-bounded channel.
type MemoryBoundedChannel[T any] struct {
	mu           sync.RWMutex
	size         int        // current memory size used by buffered items
	capacity     int        // memory limit in bytes
	buffer       *list.List // buffered items to receive (FIFO)
	notFullCond  *sync.Cond // signals when memory is not full
	notEmptyCond *sync.Cond // signals when buffer is not empty
	closed       bool       // closed flag
}

// NewMemoryBoundedChannel creates a new memory-bounded channel.
func NewMemoryBoundedChannel[T any](capacity int) *MemoryBoundedChannel[T] {
	if capacity <= 0 {
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
func (m *MemoryBoundedChannel[T]) Send(item types.Sized[T]) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		if m.closed {
			panic("send on closed channel")
		}
		if !(m.size+item.Size > m.capacity && m.buffer.Len() > 0) {
			break
		}
		m.notFullCond.Wait()
	}
	m.enqueue(item)
}

// TrySend attempts to send without blocking. Returns false if over memory limit.
func (m *MemoryBoundedChannel[T]) TrySend(item types.Sized[T]) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		panic("send on closed channel")
	}

	if m.size+item.Size > m.capacity && m.buffer.Len() > 0 {
		return false
	}
	m.enqueue(item)
	return true
}

// Receive blocks until an item is available and returns it.
func (m *MemoryBoundedChannel[T]) Receive() (v T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for !m.closed && m.buffer.Len() == 0 {
		m.notEmptyCond.Wait()
	}

	if m.buffer.Len() > 0 {
		return m.dequeue()
	}
	return
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

func (m *MemoryBoundedChannel[T]) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		m.notEmptyCond.Broadcast()
		m.notFullCond.Broadcast()
	}
}

// Closed returns true if the channel is closed.
func (m *MemoryBoundedChannel[T]) Closed() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.closed
}

// enqueue adds item and updates memory.
func (m *MemoryBoundedChannel[T]) enqueue(sitem types.Sized[T]) {
	ethMetrics.DataSize().Update(int64(sitem.Size))

	m.buffer.PushBack(sitem)
	m.size += sitem.Size

	m.notEmptyCond.Broadcast()
}

// dequeue removes and returns front item, updating memory.
func (m *MemoryBoundedChannel[T]) dequeue() T {
	elem := m.buffer.Front()
	m.buffer.Remove(elem)

	sitem := elem.Value.(types.Sized[T])
	m.size -= sitem.Size

	m.notFullCond.Broadcast()
	return sitem.Value
}

// Convenience alias for evm RevertableBlockData.
type EthRevertableBlockData = RevertableBlockData[*types.EthBlockData]

func NewEthRevertableBlockData(blockData *types.EthBlockData) *EthRevertableBlockData {
	return &EthRevertableBlockData{BlockData: blockData}
}

func NewEthRevertableBlockDataWithReorg(reorgHeight uint64) *EthRevertableBlockData {
	return &EthRevertableBlockData{ReorgHeight: &reorgHeight}
}

// Convenience alias for EthBlockData channels.
type EthMemoryBoundedChannel = MemoryBoundedChannel[*EthRevertableBlockData]

func NewEthMemoryBoundedChannel(capacity int) *EthMemoryBoundedChannel {
	return NewMemoryBoundedChannel[*EthRevertableBlockData](capacity)
}
