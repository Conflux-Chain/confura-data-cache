package types

import (
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func TestBlockHashOrNumberHash(t *testing.T) {
	val := BlockHashOrNumberWithHex("0x0003")

	// value
	hash, ok, number := val.HashOrNumber()
	assert.Equal(t, common.HexToHash("0x0003"), hash)
	assert.True(t, ok)
	assert.Equal(t, uint64(0), number)

	// json marshal
	encoded, err := json.Marshal(val)
	assert.Nil(t, err)
	assert.Equal(t, "\"0x0000000000000000000000000000000000000000000000000000000000000003\"", string(encoded))

	// json unmarshal
	var decoded BlockHashOrNumber
	err = json.Unmarshal([]byte("\"0x0000000000000000000000000000000000000000000000000000000000000007\""), &decoded)
	assert.Nil(t, err)

	hash, ok, number = decoded.HashOrNumber()
	assert.Equal(t, common.HexToHash("0x0007"), hash)
	assert.True(t, ok)
	assert.Equal(t, uint64(0), number)
}

func TestBlockHashOrNumberNumber(t *testing.T) {
	val := BlockHashOrNumberWithNumber(38)

	// value
	hash, ok, number := val.HashOrNumber()
	assert.Equal(t, common.Hash{}, hash)
	assert.False(t, ok)
	assert.Equal(t, uint64(38), number)

	// json marshal
	encoded, err := json.Marshal(val)
	assert.Nil(t, err)
	assert.Equal(t, "38", string(encoded))

	// json unmarshal
	var decoded BlockHashOrNumber
	err = json.Unmarshal([]byte("77"), &decoded)
	assert.Nil(t, err)

	hash, ok, number = decoded.HashOrNumber()
	assert.Equal(t, common.Hash{}, hash)
	assert.False(t, ok)
	assert.Equal(t, uint64(77), number)
}
