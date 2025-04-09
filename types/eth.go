package types

import "github.com/openweb3/web3go/types"

// EthBlockData contains all required data in a block.
type EthBlockData struct {
	Block    *types.Block
	Receipts []*types.Receipt
	Traces   []types.LocalizedTrace
}
