package cmd

import (
	"context"
	"fmt"
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/rpc"
	"github.com/Conflux-Chain/confura-data-cache/store"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	dataSync "github.com/Conflux-Chain/confura-data-cache/sync"
	"github.com/Conflux-Chain/go-conflux-util/cmd"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var startV2Cmd = &cobra.Command{
	Use:   "startv2",
	Short: "Start data cache service to sync data from fullnode and provides RPC and gRPC service",
	Run:   startv2,
}

func init() {
	rootCmd.AddCommand(startV2Cmd)
}

func startv2(*cobra.Command, []string) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// init store
	store := mustInitStoreFromViper()
	defer store.Close()

	// data sync
	mustStartDataSync(ctx, &wg, store)

	// serve RPC
	mustStartRPC(ctx, &wg, store)

	// wait for terminate signal to shutdown gracefully
	cmd.GracefulShutdown(&wg, cancel)
}

func mustInitStoreFromViper() store.Store {
	var config leveldb.ShardingConfig
	viper.MustUnmarshalKey("store.leveldb", &config)

	return mustInitStore(config)
}

func mustInitStore(config leveldb.ShardingConfig) store.Store {
	var (
		store store.Store
		err   error
	)

	if config.ShardingBlocks == 0 {
		store, err = leveldb.NewStore(config.Config)
	} else {
		store, err = leveldb.NewShardingStore(config)
	}

	cmd.FatalIfErr(err, "Failed to create store")

	logrus.WithField("config", fmt.Sprintf("%+v", config)).Info("LevelDB database created or opened")

	return store
}

func mustStartDataSync(ctx context.Context, wg *sync.WaitGroup, store store.Writable) {
	var config dataSync.Config
	viper.MustUnmarshalKey("sync", &config)

	worker, err := dataSync.NewWorker(config, store)
	cmd.FatalIfErr(err, "Failed to create data sync worker")

	wg.Add(1)
	go worker.Run(ctx, wg)
}

func mustStartRPC(ctx context.Context, wg *sync.WaitGroup, store store.Store) {
	var config rpc.Config
	viper.MustUnmarshalKey("rpc", &config)

	wg.Add(2)
	go rpc.MustServeRPC(ctx, wg, config, store)
	go rpc.MustServeGRPC(ctx, wg, config, store)
}
