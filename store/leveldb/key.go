package leveldb

import "sync"

// KeyPool is used to pool LevelDB database key for memory saving.
type KeyPool struct {
	sync.Pool

	prefixLen int
}

// NewKeyPool creats a new key pool with given prefix and key size.
func NewKeyPool(prefix string, size int) *KeyPool {
	var pool KeyPool

	pool.New = func() any {
		return pool.create(prefix, size)
	}

	pool.prefixLen = len(prefix)

	return &pool
}

// create creates a new pooled key with given prefix and key size.
func (pool *KeyPool) create(prefix string, size int) *[]byte {
	buf := make([]byte, len(prefix)+size)
	copy(buf, []byte(prefix))
	return &buf
}

// Get returns a pooled or new created database key with given key.
//
// Note, the returned database key is of pointer type and requires
// to call Put method to return the database key to pool.
func (pool *KeyPool) Get(key []byte) *[]byte {
	buf := pool.Pool.Get().(*[]byte)
	copy((*buf)[pool.prefixLen:], key)
	return buf
}
