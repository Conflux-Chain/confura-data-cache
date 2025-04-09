package leveldb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyPool(t *testing.T) {
	pool := NewKeyPool("foo", 5)

	// new item
	item1 := pool.Get([]byte("hello"))
	assert.Equal(t, "foohello", string(*item1))

	// another new item with same value
	item2 := pool.Get([]byte("hello"))
	assert.Equal(t, "foohello", string(*item2))
	assert.False(t, item1 == item2)

	// modify and reuse item1
	copy((*item1)[len("foo"):], []byte("world"))
	pool.Put(item1)
	item3 := pool.Get([]byte("fuzzy"))
	assert.Equal(t, "foofuzzy", string(*item3))
	assert.True(t, item1 == item3)
}
