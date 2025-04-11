package extract_test

import (
	"context"
	"os"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/mcuadros/go-defaults"
	"github.com/stretchr/testify/assert"
)

func createTestStore(t *testing.T) (*leveldb.Store, func()) {
	path, err := os.MkdirTemp("", "confura-data-cache-")
	assert.Nil(t, err)

	store, err := leveldb.NewStore(path)
	assert.Nil(t, err)

	return store, func() {
		store.Close()
		os.RemoveAll(path)
	}
}

func TestEvmSync(t *testing.T) {
	store, close := createTestStore(t)
	defer close()

	conf := extract.Config{
		StartBlockNumber: store.NextBlockNumber(),
		RpcEndpoints:     []string{"http://evmtestnet-internal.confluxrpc.com"},
	}
	defaults.SetDefaults(&conf)

	extractor, err := extract.NewEvmExtractor(conf)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resChan, err := extractor.Subscribe(ctx)
	assert.NoError(t, err)

	// Here we only test one fetch and then exit
	for res := range resChan {
		assert.NotNil(t, res)
		return
	}
}
