package sync_test

import (
	"context"
	"os"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/sync"
	"github.com/mcuadros/go-defaults"
	"github.com/openweb3/web3go"
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

	client, err := web3go.NewClient("http://evmtestnet-internal.confluxrpc.com")
	assert.NoError(t, err)

	var conf sync.Config
	defaults.SetDefaults(&conf)
	syncer, err := sync.NewEvmSyncer(store, []*web3go.Client{client}, conf)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resChan, err := syncer.Sync(ctx)
	assert.NoError(t, err)

	// Test only one fetch and then exit
	for res := range resChan {
		assert.NotNil(t, res)
		return
	}
}
