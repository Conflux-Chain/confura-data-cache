package rpc

import (
	"context"
	"encoding/json"

	pb "github.com/Conflux-Chain/confura-data-cache/rpc/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApiProto is the eth gRPC implementation.
type ApiProto struct {
	pb.UnimplementedEthServer

	inner *Api
}

func NewApiProto(api *Api) *ApiProto {
	return &ApiProto{
		inner: api,
	}
}

func (api *ApiProto) GetBlock(ctx context.Context, req *pb.GetBlockRequest) (*pb.DataResponse, error) {
	bhon := req.GetBlockId().ToBlockHashOrNumber()

	block, err := api.inner.GetBlock(bhon, req.GetIsFull())
	if err != nil {
		return nil, api.storeError(err)
	}

	return api.jsonResponse(block)
}

func (api *ApiProto) GetBlockTransactionCount(ctx context.Context, req *pb.BlockId) (*pb.GetBlockTransactionCountResponse, error) {
	bhon := req.ToBlockHashOrNumber()

	count, err := api.inner.GetBlockTransactionCount(bhon)
	if err != nil {
		return nil, api.storeError(err)
	}

	return &pb.GetBlockTransactionCountResponse{Count: count}, nil
}

func (api *ApiProto) GetTransactionByHash(ctx context.Context, req *pb.TransactionId) (*pb.DataResponse, error) {
	hash := req.ToHash()

	tx, err := api.inner.GetTransactionByHash(hash)
	if err != nil {
		return nil, api.storeError(err)
	}

	return api.jsonResponse(tx)
}

func (api *ApiProto) GetTransactionByIndex(ctx context.Context, req *pb.GetTransactionByIndexRequest) (*pb.DataResponse, error) {
	bhon := req.GetBlockId().ToBlockHashOrNumber()

	tx, err := api.inner.GetTransactionByIndex(bhon, req.GetTxIndex())
	if err != nil {
		return nil, api.storeError(err)
	}

	return api.jsonResponse(tx)
}

func (api *ApiProto) GetTransactionReceipt(ctx context.Context, req *pb.TransactionId) (*pb.DataResponse, error) {
	hash := req.ToHash()

	receipt, err := api.inner.GetTransactionReceipt(hash)
	if err != nil {
		return nil, api.storeError(err)
	}

	return api.jsonResponse(receipt)
}

func (api *ApiProto) GetBlockReceipts(ctx context.Context, req *pb.BlockId) (*pb.DataResponse, error) {
	bhon := req.ToBlockHashOrNumber()

	receipts, err := api.inner.GetBlockReceipts(bhon)
	if err != nil {
		return nil, api.storeError(err)
	}

	return api.jsonResponse(receipts)
}

func (api *ApiProto) GetTransactionTraces(ctx context.Context, req *pb.TransactionId) (*pb.DataResponse, error) {
	hash := req.ToHash()

	traces, err := api.inner.GetTransactionTraces(hash)
	if err != nil {
		return nil, api.storeError(err)
	}

	return api.jsonResponse(traces)
}

func (api *ApiProto) GetBlockTraces(ctx context.Context, req *pb.BlockId) (*pb.DataResponse, error) {
	bhon := req.ToBlockHashOrNumber()

	traces, err := api.inner.GetBlockTraces(bhon)
	if err != nil {
		return nil, api.storeError(err)
	}

	return api.jsonResponse(traces)
}

func (api *ApiProto) storeError(cause error) error {
	return status.Errorf(codes.Internal, "Store error: %v", cause.Error())
}

func (api *ApiProto) jsonResponse(v any) (*pb.DataResponse, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to marshal object: %v", v)
	}

	return &pb.DataResponse{Data: data}, nil
}
