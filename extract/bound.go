package extract

import (
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/types"
)

// Sizable represents types that report their memory footprint.
type Sizable interface {
	Size() uint64
}

// MemoryBoundedChannel wraps a buffered channel and enforces memory usage limits.
type MemoryBoundedChannel[T Sizable] struct {
	mu    sync.Mutex // guards memory usage
	cond  *sync.Cond // signals when memory is released
	ch    chan T     // underlying buffered channel
	used  uint64     // current memory used by buffered items
	limit uint64     // memory limit in bytes (0 = unlimited)
}

// NewMemoryBoundedChannel constructs a bounded channel with memory constraints.
func NewMemoryBoundedChannel[T Sizable](size int, limit uint64) *MemoryBoundedChannel[T] {
	m := &MemoryBoundedChannel[T]{
		limit: limit,
		ch:    make(chan T, size),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// Send blocks until enough memory is available and the item can be sent.
func (m *MemoryBoundedChannel[T]) Send(t T) {
	m.reserve(t.Size())
	m.ch <- t
}

// TrySend attempts to send the item without blocking.
// Returns false if over memory limit or channel is full.
func (m *MemoryBoundedChannel[T]) TrySend(t T) bool {
	size := t.Size()

	if !m.tryReserve(size) {
		return false
	}

	select {
	case m.ch <- t:
		return true
	default: // channel is full, rollback reservation
		m.release(size)
		return false
	}
}

// Receive reads an item from the channel and updates memory usage.
func (m *MemoryBoundedChannel[T]) Receive() T {
	t := <-m.ch
	m.release(t.Size())
	return t
}

// TryReceive attempts to read from the channel without blocking.
func (m *MemoryBoundedChannel[T]) TryReceive() (T, bool) {
	select {
	case t := <-m.ch:
		m.release(t.Size())
		return t, true
	default:
		var zero T
		return zero, false
	}
}

// RChan returns a read-only channel for select-style receive, with memory tracking.
func (m *MemoryBoundedChannel[T]) RChan() <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for t := range m.ch {
			out <- t
			m.release(t.Size())
		}
	}()
	return out
}

// Close closes the channel.
func (m *MemoryBoundedChannel[T]) Close() {
	close(m.ch)
}

// reserve blocks until memory for the given size can be reserved.
func (m *MemoryBoundedChannel[T]) reserve(size uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for m.limit > 0 && m.used+size > m.limit && len(m.ch) > 0 {
		m.cond.Wait()
	}
	m.used += size
}

// tryReserve attempts to reserve memory for the given size without blocking.
func (m *MemoryBoundedChannel[T]) tryReserve(size uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.limit > 0 && m.used+size > m.limit && len(m.ch) > 0 {
		return false
	}
	m.used += size
	return true
}

// release deducts memory usage and signals waiting Senders.
func (m *MemoryBoundedChannel[T]) release(size uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.used -= size
	m.cond.Broadcast()
}

type EthMemoryBoundedChannel = MemoryBoundedChannel[*types.EthBlockData]

func NewEthMemoryBoundedChannel(size int, limit uint64) *EthMemoryBoundedChannel {
	return NewMemoryBoundedChannel[*types.EthBlockData](size, limit)
}
