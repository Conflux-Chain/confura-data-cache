package rpc

import (
	"context"
	"time"

	pb "github.com/Conflux-Chain/confura-data-cache/rpc/proto"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/openweb3/web3go/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ Interface = (*ClientProto)(nil)

type ClientProto struct {
	inner pb.EthClient

	url string

	timeout time.Duration
}

func NewClientProto(url string, timeout ...time.Duration) (*ClientProto, error) {
	conn, err := grpc.Dial(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	callTimeout := 3 * time.Second
	if len(timeout) > 0 {
		callTimeout = timeout[0]
	}

	return &ClientProto{
		inner:   pb.NewEthClient(conn),
		url:     url,
		timeout: callTimeout,
	}, nil
}

func (c *ClientProto) String() string {
	return c.url
}

func (c *ClientProto) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetBlock(ctx, &pb.GetBlockRequest{
		BlockId: pb.NewBlockId(bhon),
		IsFull:  isFull,
	})

	return pb.ResponseToLazy[*ethTypes.Block](resp, err)
}

func (c *ClientProto) GetBlockTransactionCount(bhon types.BlockHashOrNumber) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetBlockTransactionCount(ctx, pb.NewBlockId(bhon))
	if err != nil {
		return 0, err
	}

	return resp.GetCount(), nil
}

func (c *ClientProto) GetTransactionByHash(txHash common.Hash) (types.Lazy[*ethTypes.TransactionDetail], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetTransactionByHash(ctx, pb.NewTransactionId(txHash))

	return pb.ResponseToLazy[*ethTypes.TransactionDetail](resp, err)
}

func (c *ClientProto) GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (types.Lazy[*ethTypes.TransactionDetail], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetTransactionByIndex(ctx, &pb.GetTransactionByIndexRequest{
		BlockId: pb.NewBlockId(bhon),
		TxIndex: txIndex,
	})

	return pb.ResponseToLazy[*ethTypes.TransactionDetail](resp, err)
}

func (c *ClientProto) GetTransactionReceipt(txHash common.Hash) (types.Lazy[*ethTypes.Receipt], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetTransactionReceipt(ctx, pb.NewTransactionId(txHash))

	return pb.ResponseToLazy[*ethTypes.Receipt](resp, err)
}

func (c *ClientProto) GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetBlockReceipts(ctx, pb.NewBlockId(bhon))

	return pb.ResponseToLazy[[]ethTypes.Receipt](resp, err)
}

func (c *ClientProto) GetTransactionTraces(txHash common.Hash) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetTransactionTraces(ctx, pb.NewTransactionId(txHash))

	return pb.ResponseToLazy[[]ethTypes.LocalizedTrace](resp, err)
}

func (c *ClientProto) GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetBlockTraces(ctx, pb.NewBlockId(bhon))

	return pb.ResponseToLazy[[]ethTypes.LocalizedTrace](resp, err)
}

func (c *ClientProto) GetTrace(txHash common.Hash, index uint) (types.Lazy[*ethTypes.LocalizedTrace], error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := c.inner.GetTrace(ctx, &pb.GetTraceRequest{
		TransactionId: pb.NewTransactionId(txHash),
		Index:         uint32(index),
	})

	return pb.ResponseToLazy[*ethTypes.LocalizedTrace](resp, err)
}
