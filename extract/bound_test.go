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

func TestMemoryBoundedChannelSendAfterClosePanics(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)
	mc.Close()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic when sending to closed channel, but did not panic")
		}
	}()

	mc.Send(mockItem{id: 999, size: 10}) // should panic
}

func TestMemoryBoundedChannelReceiveAfterClose(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 10, size: 30}
	mc.Send(item)

	// Close channel
	mc.Close()

	// Receive should return the last item
	received := mc.Receive()
	assert.Equal(t, item, received)

	// After buffer is empty, Receive should return zero value immediately
	defaultItem := mc.Receive()
	assert.Equal(t, mockItem{}, defaultItem)

	// Another TryReceive should return false
	_, ok := mc.TryReceive()
	assert.False(t, ok)
}

func TestMemoryBoundedChannelCloseWakesBlockedSend(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](10)

	// Fill buffer to near capacity
	mc.Send(mockItem{id: 1, size: 9})

	blockingItem := mockItem{id: 2, size: 5}
	done := make(chan struct{})

	go func() {
		mc.Send(blockingItem) // This will block
		close(done)
	}()

	// Ensure the goroutine is blocked
	time.Sleep(50 * time.Millisecond)

	// Close the channel to unblock the sender
	mc.Close()

	select {
	case <-done:
		// Pass
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Send() did not unblock after Close()")
	}
}

func TestMemoryBoundedChannelCloseIsIdempotent(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	// Should not panic or deadlock
	mc.Close()
	mc.Close()
	mc.Close()
}
