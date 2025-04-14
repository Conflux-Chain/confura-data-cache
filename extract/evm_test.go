package extract_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/mcuadros/go-defaults"
	"github.com/stretchr/testify/assert"
)

func TestEvmExtract(t *testing.T) {
	endpoints := os.Getenv("TEST_EVM_RPC_ENDPOINTS")
	if len(endpoints) == 0 {
		t.Skip("no rpc endpoints provided, skip test")
		return
	}

	conf := extract.Config{
		RpcEndpoints: strings.Split(endpoints, ","),
	}
	defaults.SetDefaults(&conf)

	extractor, err := extract.NewEvmExtractor(conf)
	assert.NoError(t, err)
	defer extractor.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resChan, err := extractor.Subscribe(ctx)
	assert.NoError(t, err)

	<-resChan

	assert.NoError(t, extractor.Unsubscribe())
}
