package extract

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/stretchr/testify/assert"
)

// mockItem is a test type that implements the Sizable interface.
type mockItem struct {
	id   int
	size int
}

func (m mockItem) Size() int {
	return m.size
}

func TestMemoryBoundedChannelSendAndReceive(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 1, size: 42}
	mc.Send(types.NewSized(item))
	assert.Equal(t, 1, mc.Len())

	received, err := mc.Receive()
	assert.NoError(t, err)
	assert.Equal(t, item, received)
	assert.Equal(t, 0, mc.Len())
}

func TestMemoryBoundedChannelTrySendSuccess(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 2, size: 50}
	ok, err := mc.TrySend(types.NewSized(item))
	assert.NoError(t, err)
	assert.True(t, ok)

	out, err := mc.Receive()
	assert.NoError(t, err)
	assert.Equal(t, item, out)
}

func TestMemoryBoundedChannelTrySendFailDueToMemoryFull(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 1, size: 2}
	ok, err := mc.TrySend(types.NewSized(item))
	assert.NoError(t, err)
	assert.True(t, ok)

	item = mockItem{id: 2, size: 200}
	ok, err = mc.TrySend(types.NewSized(item))
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestMemoryBoundedChannelTrySendClosed(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)
	mc.Close()

	item := mockItem{id: 1, size: 2}
	ok, err := mc.TrySend(types.NewSized(item))
	assert.ErrorIs(t, err, ErrChannelClosed)
	assert.False(t, ok)
}

func TestMemoryBoundedChannelTryReceive(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 5, size: 30}
	ok, err := mc.TrySend(types.NewSized(item))
	assert.NoError(t, err)
	assert.True(t, ok)

	got, ok, err := mc.TryReceive()
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, item, got)

	_, ok, err = mc.TryReceive()
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestMemoryBoundedChannelBlockingSendUnderLimit(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	item := mockItem{id: 7, size: 100}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		mc.Send(types.NewSized(item))
	}()

	time.Sleep(50 * time.Millisecond)
	got, err := mc.Receive()
	assert.NoError(t, err)
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
			mc.Send(types.NewSized(mockItem{id: i, size: 50 * i}))
		}
	}()

	// Receiver
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range numItems {
			item, err := mc.Receive()
			assert.NoError(t, err)
			assert.Equal(t, i, item.id)
			assert.Equal(t, 50*i, item.Size())
		}
	}()

	wg.Wait()
}

func TestMemoryBoundedChannelSendAfterClose(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)
	mc.Close()

	err := mc.Send(types.NewSized(mockItem{id: 999, size: 10}))
	assert.ErrorIs(t, err, ErrChannelClosed)
}

func TestMemoryBoundedChannelReceiveAfterClose(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)
	assert.False(t, mc.Closed())

	item := mockItem{id: 10, size: 30}
	mc.Send(types.NewSized(item))

	// Close channel
	mc.Close()
	assert.True(t, mc.Closed())

	// Receive should return the last item
	received, err := mc.Receive()
	assert.NoError(t, err)
	assert.Equal(t, item, received)

	// After buffer is empty, Receive should return zero value immediately
	defaultItem, err := mc.Receive()
	assert.ErrorIs(t, err, ErrChannelClosed)
	assert.Equal(t, mockItem{}, defaultItem)

	// Another TryReceive should return false
	_, ok, err := mc.TryReceive()
	assert.ErrorIs(t, err, ErrChannelClosed)
	assert.False(t, ok)
}

func TestMemoryBoundedChannelCloseWakesBlockedReceive(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	done := make(chan struct{})

	go func() {
		defer close(done)
		item, err := mc.Receive()
		assert.ErrorIs(t, err, ErrChannelClosed)
		// Since channel is closed and empty, we expect zero value
		assert.Equal(t, mockItem{}, item)
	}()

	// Give goroutine time to block on Receive
	time.Sleep(50 * time.Millisecond)

	// Closing should wake the blocked Receive
	mc.Close()

	select {
	case <-done:
		// Pass
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Receive() did not unblock after Close()")
	}
}

func TestMemoryBoundedChannelCloseIsIdempotent(t *testing.T) {
	mc := NewMemoryBoundedChannel[mockItem](100)

	// Should not panic or deadlock
	mc.Close()
	mc.Close()
	mc.Close()
}

func TestMemoryBoundedChannelRChan(t *testing.T) {
	t.Run("RChanReadOk", func(t *testing.T) {
		mc := NewMemoryBoundedChannel[mockItem](100)
		item := mockItem{id: 1, size: 42}
		mc.Send(types.NewSized(item))
		assert.Equal(t, 1, mc.Len())

		received, ok := <-mc.RChan(context.Background())
		assert.True(t, ok)
		assert.Equal(t, item, received)
		assert.Equal(t, 0, mc.Len())
	})

	t.Run("RchanReadClosed", func(t *testing.T) {
		mc := NewMemoryBoundedChannel[mockItem](100)
		item := mockItem{id: 1, size: 10}
		mc.Send(types.NewSized(item))
		assert.Equal(t, 1, mc.Len())

		// Close the channel
		mc.Close()
		assert.Equal(t, 1, mc.Len())

		// Attempt to receive from the closed channel
		received, ok := <-mc.RChan(context.Background())
		assert.True(t, ok)
		assert.Equal(t, item, received)
		assert.Equal(t, 0, mc.Len())

		// Attempt to receive from the empty closed channel again
		received, ok = <-mc.RChan(context.Background())
		assert.False(t, ok)
		assert.Equal(t, mockItem{}, received)
		assert.Equal(t, 0, mc.Len())
	})

	t.Run("RChanContextCanceledImmediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel the context immediately

		mc := NewMemoryBoundedChannel[mockItem](100)
		item := mockItem{id: 1, size: 42}
		mc.Send(types.NewSized(item))
		assert.Equal(t, 1, mc.Len())

		received, ok := <-mc.RChan(ctx)
		assert.False(t, ok)
		assert.Equal(t, mockItem{}, received)
		assert.Equal(t, 0, mc.Len())
	})

	t.Run("RChanContextCanceledIntermediately", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		mc := NewMemoryBoundedChannel[mockItem](100)
		item := mockItem{id: 1, size: 42}
		mc.Send(types.NewSized(item))
		assert.Equal(t, 1, mc.Len())

		ch := mc.RChan(ctx)

		time.Sleep(time.Millisecond)
		cancel() // Cancel the context
		time.Sleep(time.Millisecond)

		received, ok := <-ch
		assert.False(t, ok)
		assert.Equal(t, mockItem{}, received)
		assert.Equal(t, 0, mc.Len())
	})
}
