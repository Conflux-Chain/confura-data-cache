package leveldb

import (
	"encoding/binary"
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

func (store *Store) readRaw(key []byte, expectedValueSize ...int) ([]byte, bool, error) {
	value, err := store.db.Get(key, nil)
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

func (store *Store) read(pool *KeyPool, key []byte, expectedValueSize ...int) ([]byte, bool, error) {
	prefixedKey := pool.Get(key)
	defer pool.Put(prefixedKey)

	return store.readRaw(*prefixedKey, expectedValueSize...)
}

func (store *Store) readUint64(key []byte) (uint64, bool, error) {
	value, ok, err := store.readRaw(key, 8)
	if err != nil || !ok {
		return 0, false, err
	}

	return binary.BigEndian.Uint64(value), true, nil
}

func (store *Store) readJson(key []byte, valPtr any) (bool, error) {
	value, ok, err := store.readRaw(key)
	if err != nil || !ok {
		return false, err
	}

	if err = json.Unmarshal(value, valPtr); err != nil {
		return false, errors.WithMessage(err, "Failed to unmarshal JSON")
	}

	return true, nil
}
