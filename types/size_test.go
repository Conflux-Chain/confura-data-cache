package types

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

type SizableBool bool

func (SizableBool) Size() int {
	return 777
}

func TestSizeCustom(t *testing.T) {
	o := NewSized(SizableBool(true), 999)
	assert.Equal(t, 999, o.Size)
}

func TestSizeInterface(t *testing.T) {
	o := NewSized(SizableBool(true))
	assert.Equal(t, 777, o.Size)
}

func TestSizeReflection(t *testing.T) {
	var data struct {
		Address  common.Address //20
		AddressP *common.Address
		Big      *big.Int
		Bytes    []byte
		Uint64   uint64      // 8
		Hash     common.Hash //32
		HashP    *common.Hash
	}

	size0 := NewSized(data).Size
	assert.GreaterOrEqual(t, size0, 20+8+32)

	data.Big = big.NewInt(3)     // 9
	data.Bytes = []byte("hello") // 5
	size1 := NewSized(data).Size
	assert.GreaterOrEqual(t, size1, size0+9+5)

	data.AddressP = &common.Address{}
	data.HashP = &common.Hash{}
	size2 := NewSized(data).Size
	assert.GreaterOrEqual(t, size2, size1+20+32)
}
