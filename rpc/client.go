package rpc

import (
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/ethereum/go-ethereum/common"
	providers "github.com/openweb3/go-rpc-provider/provider_wrapper"
	ethTypes "github.com/openweb3/web3go/types"
)

var _ Interface = (*Client)(nil)

// Client is the RPC client to interact with RPC server.
type Client struct {
	*providers.MiddlewarableProvider

	url string
}

// NewClient creates a new client instance.
func NewClient(url string, option ...providers.Option) (*Client, error) {
	var opt providers.Option
	if len(option) > 0 {
		opt = option[0]
	}

	provider, err := providers.NewProviderWithOption(url, opt)
	if err != nil {
		return nil, err
	}

	return &Client{provider, url}, nil
}

// String returns the URL of RPC server.
func (c *Client) String() string {
	return c.url
}

func (c *Client) GetBlock(bhon types.BlockHashOrNumber, isFull bool) (types.Lazy[*ethTypes.Block], error) {
	return providers.Call[types.Lazy[*ethTypes.Block]](c, "eth_getBlock", bhon, isFull)
}

func (c *Client) GetBlockTransactionCount(bhon types.BlockHashOrNumber) (int64, error) {
	return providers.Call[int64](c, "eth_getBlockTransactionCount", bhon)
}

func (c *Client) GetTransactionByHash(txHash common.Hash) (types.Lazy[*ethTypes.TransactionDetail], error) {
	return providers.Call[types.Lazy[*ethTypes.TransactionDetail]](c, "eth_getTransactionByHash", txHash)
}

func (c *Client) GetTransactionByIndex(bhon types.BlockHashOrNumber, txIndex uint32) (types.Lazy[*ethTypes.TransactionDetail], error) {
	return providers.Call[types.Lazy[*ethTypes.TransactionDetail]](c, "eth_getTransactionByIndex", bhon, txIndex)
}

func (c *Client) GetTransactionReceipt(txHash common.Hash) (types.Lazy[*ethTypes.Receipt], error) {
	return providers.Call[types.Lazy[*ethTypes.Receipt]](c, "eth_getTransactionReceipt", txHash)
}

func (c *Client) GetBlockReceipts(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.Receipt], error) {
	return providers.Call[types.Lazy[[]ethTypes.Receipt]](c, "eth_getBlockReceipts", bhon)
}

func (c *Client) GetTransactionTraces(txHash common.Hash) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	return providers.Call[types.Lazy[[]ethTypes.LocalizedTrace]](c, "eth_getTransactionTraces", txHash)
}

func (c *Client) GetBlockTraces(bhon types.BlockHashOrNumber) (types.Lazy[[]ethTypes.LocalizedTrace], error) {
	return providers.Call[types.Lazy[[]ethTypes.LocalizedTrace]](c, "eth_getBlockTraces", bhon)
}

func (c *Client) GetTrace(txHash common.Hash, index uint) (types.Lazy[*ethTypes.LocalizedTrace], error) {
	return providers.Call[types.Lazy[*ethTypes.LocalizedTrace]](c, "eth_getTrace", txHash, index)
}
