package rpc

import (
	"encoding/json"
	reflect "reflect"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
)

func NewBlockId(bhon types.BlockHashOrNumber) *BlockId {
	hash, ok, number := bhon.HashOrNumber()
	if ok {
		return &BlockId{Hash: hash.Bytes()}
	}

	return &BlockId{Number: number}
}

func (id *BlockId) ToBlockHashOrNumber() types.BlockHashOrNumber {
	if hash := id.GetHash(); len(hash) > 0 {
		return types.BlockHashOrNumberWithHash(common.BytesToHash(hash))
	}

	return types.BlockHashOrNumberWithNumber(id.GetNumber())
}

func NewTransactionId(hash common.Hash) *TransactionId {
	return &TransactionId{Hash: hash.Bytes()}
}

func (id *TransactionId) ToHash() common.Hash {
	hash := id.GetHash()
	return common.BytesToHash(hash)
}

func ResponseToLazy[T any](resp *DataResponse, err error) (types.Lazy[T], error) {
	if err != nil {
		return types.Lazy[T]{}, err
	}

	var result types.Lazy[T]

	if err := json.Unmarshal(resp.GetData(), &result); err != nil {
		return types.Lazy[T]{}, errors.WithMessagef(err, "Failed to unmarshal lazy object of generic type: %v", reflect.TypeFor[T]())
	}

	return result, nil
}
