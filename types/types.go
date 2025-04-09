package types

import (
	cfxTypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
)

// ChainData is a generic type for blockchain data including block, receipts and traces.
type ChainData[TB, TR, TT any] struct {
	Block    TB
	Receipts TR
	Traces   []TT
}

// BlockData is a generic interface for block data.
type BlockData interface {
	EthBlockData | CfxBlockData

	BlockNumber() uint64
	BlockHash() common.Hash
}

// EthBlockData contains all required data for EVM compatible blocks.
type EthBlockData ChainData[*ethTypes.Block, *ethTypes.Receipt, ethTypes.LocalizedTrace]

func (data EthBlockData) BlockNumber() uint64 {
	return data.Block.Number.Uint64()
}

func (data EthBlockData) BlockHash() common.Hash {
	return data.Block.Hash
}

// CfxBlockData contains all required data for Conflux core space blocks.
type CfxBlockData ChainData[*cfxTypes.Block, *cfxTypes.TransactionReceipt, cfxTypes.LocalizedTrace]

func (data CfxBlockData) BlockNumber() uint64 {
	return data.Block.BlockNumber.ToInt().Uint64()
}

func (data CfxBlockData) BlockHash() common.Hash {
	return *data.Block.Hash.ToCommonHash()
}
