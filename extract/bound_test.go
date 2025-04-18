package extract

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// mockItem is a test type that implements the Sizable interface.
type mockItem struct {
	id   int
	size uint64
}

func (m mockItem) Size() uint64 {
	return m.size
}

func TestMemoryBoundedChannelSendAndReceive(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 1, size: 42}
	mc.Send(item)
	assert.Equal(t, 1, mc.Len())

	received := mc.Receive()
	assert.Equal(t, item, received)
	assert.Equal(t, 0, mc.Len())
}

func TestMemoryBoundedChannelTrySendSuccess(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 2, size: 50}
	ok := mc.TrySend(item)
	assert.True(t, ok)

	out := mc.Receive()
	assert.Equal(t, item, out)
}

func TestMemoryBoundedChannelTrySendFailDueToMemoryFull(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 1, size: 2}
	ok := mc.TrySend(item)
	assert.True(t, ok)

	item = mockItem{id: 2, size: 200}
	ok = mc.TrySend(item)
	assert.False(t, ok)
}

func TestMemoryBoundedChannelTryReceive(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 5, size: 30}
	assert.True(t, mc.TrySend(item))

	got, ok := mc.TryReceive()
	assert.True(t, ok)
	assert.Equal(t, item, got)

	_, ok = mc.TryReceive()
	assert.False(t, ok)
}

func TestMemoryBoundedChannelBlockingSendUnderLimit(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 7, size: 100}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		mc.Send(item)
	}()

	time.Sleep(50 * time.Millisecond)
	got := mc.Receive()
	assert.Equal(t, item, got)

	wg.Wait()
}

func TestMemoryBoundedChannelConcurrentSendReceive(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	numItems := 5
	var wg sync.WaitGroup

	// Sender
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range numItems {
			mc.Send(mockItem{id: i, size: uint64(50 * i)})
		}
	}()

	// Receiver
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range numItems {
			item := mc.Receive()
			assert.Equal(t, i, item.id)
			assert.Equal(t, uint64(50*i), item.Size())
		}
	}()

	wg.Wait()
}
