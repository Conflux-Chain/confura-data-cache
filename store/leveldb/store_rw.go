package leveldb

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	dberrors "github.com/syndtr/goleveldb/leveldb/errors"
)

/////////////////////////////////////////////////////////////////////////////////////
//
//                Read/Write Utils
//
/////////////////////////////////////////////////////////////////////////////////////

func (*Store) write(batch *leveldb.Batch, pool *KeyPool, key, value []byte) {
	prefixedKey := pool.Get(key)
	defer pool.Put(prefixedKey)

	batch.Put(*prefixedKey, value)
}

func (store *Store) writeJson(batch *leveldb.Batch, pool *KeyPool, key []byte, value any) {
	valueJson, _ := json.Marshal(value)

	store.write(batch, pool, key, valueJson)
}

func (store *Store) read(pool *KeyPool, key []byte, expectedValueSize ...int) ([]byte, bool, error) {
	prefixedKey := pool.Get(key)
	defer pool.Put(prefixedKey)

	value, err := store.db.Get(*prefixedKey, nil)
	if err == dberrors.ErrNotFound {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	// check value size
	if len(expectedValueSize) > 0 && len(value) != expectedValueSize[0] {
		return nil, false, errors.Errorf("Invalid value size, expected = %v, actual = %v", expectedValueSize[0], len(value))
	}

	return value, true, nil
}

func (store *Store) readJson(pool *KeyPool, key []byte, valuePointer any) (bool, error) {
	value, ok, err := store.read(pool, key)
	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	if err = json.Unmarshal(value, valuePointer); err != nil {
		return false, errors.WithMessage(err, "Failed to unmarshal JSON value to object")
	}

	return true, nil
}
