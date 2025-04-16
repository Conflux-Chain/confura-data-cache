package extract_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/mcuadros/go-defaults"
	"github.com/stretchr/testify/assert"
)

func TestEvmExtract(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("TEST_EVM_RPC_ENDPOINT"))
	if len(endpoint) == 0 {
		t.Skip("no rpc endpoint provided, skip test")
		return
	}

	conf := extract.EthConfig{RpcEndpoint: endpoint}
	defaults.SetDefaults(&conf)

	extractor, err := extract.NewEvmExtractor(conf)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dataChan := make(chan types.EthBlockData, 1)
	go extractor.Start(ctx, dataChan)

	data := <-dataChan

	assert.NotNil(t, data)
	assert.NotNil(t, data.Block)
	assert.NotNil(t, data.Receipts)
	assert.NotNil(t, data.Traces)
}
