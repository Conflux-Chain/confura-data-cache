package types

import (
	"encoding/json"
	"errors"
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

func TestIsRpcMethodNotSupportedError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "method not found, lowercase",
			err:      errors.New("method eth_xyz not found"),
			expected: true,
		},
		{
			name:     "method not exist, mixed case",
			err:      errors.New("Method abc does NOT exist"),
			expected: true,
		},
		{
			name:     "method not available, uppercase",
			err:      errors.New("METHOD trace_foo NOT AVAILABLE"),
			expected: true,
		},
		{
			name:     "unrelated error message",
			err:      errors.New("some internal error"),
			expected: false,
		},
		{
			name:     "method not supported error",
			err:      errors.New("the method eth_getBlockByNumbers does not exist/is not available"),
			expected: true,
		},
		{
			name:     "method not found",
			err:      errors.New("Method not found"),
			expected: true,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isRpcMethodNotSupportedError(c.err)
			if got != c.expected {
				t.Errorf("Expected %v, got %v", c.expected, got)
			}
		})
	}
}
