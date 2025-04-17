package extract

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/Conflux-Chain/confura-data-cache/types"
)

// Sizable represents types that report their memory footprint.
type Sizable interface {
	Size() uint64
}

// MemoryBoundedChannel wraps a channel and enforces a memory usage limit.
type MemoryBoundedChannel[T Sizable] struct {
	ch    chan T        // underlying buffered channel
	used  atomic.Uint64 // total memory used by items in the channel
	limit uint64        // memory limit in bytes (0 = unlimited)
}

// NewMemoryBoundedChannel creates a new memory-bounded channel wrapper.
func NewMemoryBoundedChannel[T Sizable](ch chan T, limit uint64) *MemoryBoundedChannel[T] {
	return &MemoryBoundedChannel[T]{ch: ch, limit: limit}
}

// Send blocks until it can send the item without exceeding the memory limit.
func (m *MemoryBoundedChannel[T]) Send(t T) {
	size := t.Size()
	for {
		if m.tryReserve(size) {
			m.ch <- t
			return
		}
		runtime.Gosched()
	}
}

// TrySend attempts to send the item without blocking.
func (m *MemoryBoundedChannel[T]) TrySend(t T) bool {
	size := t.Size()
	if !m.tryReserve(size) {
		return false
	}
	select {
	case m.ch <- t:
		return true
	default:
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
func (m *MemoryBoundedChannel[T]) RChan(ctx context.Context) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-m.ch:
				out <- t
				m.release(t.Size())
			}
		}
	}()
	return out
}

// tryReserve attempts to reserve memory; returns true if successful.
func (m *MemoryBoundedChannel[T]) tryReserve(size uint64) bool {
	for {
		mem := m.used.Load()
		if m.limit > 0 && mem+size > m.limit && len(m.ch) > 0 {
			return false
		}
		if m.used.CompareAndSwap(mem, mem+size) {
			return true
		}
	}
}

// release decrements memory usage after receiving an item.
func (m *MemoryBoundedChannel[T]) release(size uint64) {
	m.used.Add(^uint64(size - 1))
}

type EthMemoryBoundedChannel = MemoryBoundedChannel[*types.EthBlockData]
