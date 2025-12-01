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
	viperUtil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start data cache service to sync data from fullnode and provides RPC and gRPC service",
	Run:   start,
}

func init() {
	startCmd.Flags().String("store-path", leveldb.DefaultConfig().Path, "LevelDB database path")
	viper.BindPFlag("store.leveldb.path", startCmd.Flag("store-path"))

	rootCmd.AddCommand(startCmd)
}

func start(*cobra.Command, []string) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	store := mustInitStore()
	defer store.Close()

	// run sync
	syncer := dataSync.MustNewEthSyncerFromViper(store)

	wg.Add(1)
	go syncer.Run(ctx, &wg)

	// serve RPC
	var rpcConfig rpc.Config
	viperUtil.MustUnmarshalKey("rpc", &rpcConfig)
	wg.Add(2)
	go rpc.MustServeRPC(ctx, &wg, rpcConfig, store)
	go rpc.MustServeGRPC(ctx, &wg, rpcConfig, store)

	// wait for terminate signal to shutdown gracefully
	cmd.GracefulShutdown(&wg, cancel)
}

func mustInitStore() store.Store {
	var config leveldb.ShardingConfig
	viperUtil.MustUnmarshalKey("store.leveldb", &config)

	if config.ShardingBlocks == 0 {
		store, err := leveldb.NewStore(config.Config)
		cmd.FatalIfErr(err, "Failed to create LevelDB database")
		return store
	}

	store, err := leveldb.NewShardingStore(config)
	cmd.FatalIfErr(err, "Failed to create sharding LevelDB database")

	logrus.WithField("config", fmt.Sprintf("%+v", config)).Info("LevelDB database created or opened")

	return store
}
