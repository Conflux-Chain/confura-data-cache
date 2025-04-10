package types

import (
	"github.com/openweb3/web3go"
	"github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
)

// EthBlockData contains all required data in a block.
type EthBlockData struct {
	Block    *types.Block
	Receipts []types.Receipt
	Traces   []types.LocalizedTrace
}

func QueryEthBlockData(client *web3go.Client, blockNumber uint64) (EthBlockData, error) {
	bn := types.NewBlockNumber(int64(blockNumber))
	block, err := client.Eth.BlockByNumber(bn, true)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block by number")
	}

	bnoh := types.BlockNumberOrHashWithNumber(bn)
	receipts, err := client.Parity.BlockReceipts(&bnoh)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block receipts")
	}

	traces, err := client.Trace.Blocks(bnoh)
	if err != nil {
		return EthBlockData{}, errors.WithMessage(err, "Failed to get block traces")
	}

	return EthBlockData{
		Block:    block,
		Receipts: receipts,
		Traces:   traces,
	}, nil
}
