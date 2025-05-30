package rpc

import (
	"context"

	pb "github.com/Conflux-Chain/confura-data-cache/rpc/proto"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"google.golang.org/grpc"
)

type ClientProto struct {
	inner pb.EthClient

	url string
}

func NewClientProto(url string) (*ClientProto, error) {
	conn, err := grpc.Dial(url)
	if err != nil {
		return nil, err
	}

	client := pb.NewEthClient(conn)

	return &ClientProto{client, url}, nil
}

func (c *ClientProto) String() string {
	return c.url
}

func (c *ClientProto) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
	resp, err := c.inner.GetBlock(context.Background(), &pb.GetBlockRequest{
		BlockId: pb.NewBlockId(bhon),
		IsFull:  isFull,
	})

	return pb.ResponseToLazy[*ethTypes.Block](resp, err)
}

func (c *ClientProto) GetBlockTransactionCount(bhon types.BlockHashOrNumber) (int64, error) {
	resp, err := c.inner.GetBlockTransactionCount(context.Background(), pb.NewBlockId(bhon))
	if err != nil {
		return 0, err
	}

	return resp.GetCount(), nil
}

func (c *ClientProto) GetTransactionByHash(txHash common.Hash) (types.Lazy[*ethTypes.TransactionDetail], error) {
	resp, err := c.inner.GetTransactionByHash(context.Background(), pb.NewTransactionId(txHash))

	return pb.ResponseToLazy[*ethTypes.TransactionDetail](resp, err)
}

func (c *ClientProto) GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (types.Lazy[*ethTypes.TransactionDetail], error) {
	resp, err := c.inner.GetTransactionByIndex(context.Background(), &pb.GetTransactionByIndexRequest{
		BlockId: pb.NewBlockId(bhon),
		TxIndex: txIndex,
	})

	return pb.ResponseToLazy[*ethTypes.TransactionDetail](resp, err)
}

func (c *ClientProto) GetTransactionReceipt(txHash common.Hash) (types.Lazy[*ethTypes.Receipt], error) {
	resp, err := c.inner.GetTransactionReceipt(context.Background(), pb.NewTransactionId(txHash))

	return pb.ResponseToLazy[*ethTypes.Receipt](resp, err)
}

func (c *ClientProto) GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error) {
	resp, err := c.inner.GetBlockReceipts(context.Background(), pb.NewBlockId(bhon))

	return pb.ResponseToLazy[[]ethTypes.Receipt](resp, err)
}

func (c *ClientProto) GetTransactionTraces(txHash common.Hash) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	resp, err := c.inner.GetTransactionTraces(context.Background(), pb.NewTransactionId(txHash))

	return pb.ResponseToLazy[[]ethTypes.LocalizedTrace](resp, err)
}

func (c *ClientProto) GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	resp, err := c.inner.GetBlockTraces(context.Background(), pb.NewBlockId(bhon))

	return pb.ResponseToLazy[[]ethTypes.LocalizedTrace](resp, err)
}
