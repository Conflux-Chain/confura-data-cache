package extract

import (
	"context"
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
	ch := make(chan mockItem, 10)
	mc := NewMemoryBoundedChannel(ch, 100)

	item := mockItem{id: 1, size: 42}
	mc.Send(item)

	received := mc.Receive()
	assert.Equal(t, item, received)
}

func TestMemoryBoundedChannelTrySendSuccess(t *testing.T) {
	ch := make(chan mockItem, 1)
	mc := NewMemoryBoundedChannel(ch, 100)

	item := mockItem{id: 2, size: 50}
	ok := mc.TrySend(item)
	assert.True(t, ok)

	out := mc.Receive()
	assert.Equal(t, item, out)
}

func TestMemoryBoundedChannelTrySendFailDueToLimit(t *testing.T) {
	ch := make(chan mockItem, 1)
	mc := NewMemoryBoundedChannel(ch, 10)

	item := mockItem{id: 1, size: 2}
	ok := mc.TrySend(item)
	assert.True(t, ok)

	item = mockItem{id: 2, size: 200}
	ok = mc.TrySend(item)
	assert.False(t, ok)
}

func TestMemoryBoundedChannelTrySendFailDueToFullChannel(t *testing.T) {
	ch := make(chan mockItem, 1)
	mc := NewMemoryBoundedChannel(ch, 100)

	item := mockItem{id: 4, size: 50}
	assert.True(t, mc.TrySend(item))
	assert.False(t, mc.TrySend(item)) // channel is full
}

func TestMemoryBoundedChannelTryReceive(t *testing.T) {
	ch := make(chan mockItem, 1)
	mc := NewMemoryBoundedChannel(ch, 100)

	item := mockItem{id: 5, size: 30}
	assert.True(t, mc.TrySend(item))

	got, ok := mc.TryReceive()
	assert.True(t, ok)
	assert.Equal(t, item, got)

	_, ok = mc.TryReceive()
	assert.False(t, ok)
}

func TestMemoryBoundedChannelRChan(t *testing.T) {
	ch := make(chan mockItem, 1)
	mc := NewMemoryBoundedChannel(ch, 100)

	item := mockItem{id: 6, size: 60}
	mc.Send(item)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selectCh := mc.RChan(ctx)
	select {
	case got := <-selectCh:
		assert.Equal(t, item, got)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for item on RChan")
	}
}

func TestMemoryBoundedChannelBlockingSendUnderLimit(t *testing.T) {
	ch := make(chan mockItem, 1)
	mc := NewMemoryBoundedChannel(ch, 100)

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
	ch := make(chan mockItem, 5)
	mc := NewMemoryBoundedChannel(ch, 200)

	numItems := 5
	var wg sync.WaitGroup

	// Sender
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
			mc.Send(mockItem{id: i, size: 50})
		}
	}()

	// Receiver
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
			item := mc.Receive()
			assert.Equal(t, uint64(50), item.Size())
		}
	}()

	wg.Wait()
}
