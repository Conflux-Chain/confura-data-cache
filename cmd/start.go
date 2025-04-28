package cmd

import (
	"fmt"

	"github.com/Conflux-Chain/confura-data-cache/rpc"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	viperUtil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start data cache service to sync data from fullnode and provides RPC service",
	Run:   start,
}

func init() {
	startCmd.Flags().String("store-path", leveldb.DefaultConfig().Path, "LevelDB database path")
	viper.BindPFlag("store.leveldb.path", startCmd.Flag("store-path"))

	startCmd.Flags().String("endpoint", rpc.DefaultConfig().Endpoint, "RPC endpoint to serve cached data query")
	viper.BindPFlag("rpc.endpoint", startCmd.Flag("endpoint"))

	rootCmd.AddCommand(startCmd)
}

func start(*cobra.Command, []string) {
	// create or open leveldb database
	var storeConfig leveldb.Config
	viperUtil.MustUnmarshalKey("store.leveldb", &storeConfig)
	store, err := leveldb.NewStore(storeConfig)
	if err != nil {
		logrus.WithError(err).WithField("config", storeConfig).Fatal("Failed to create LevelDB database")
	}
	defer store.Close()
	logrus.WithField("config", fmt.Sprintf("%+v", storeConfig)).Info("LevelDB database created or opened")

	// TODO go sync.Run(store)

	// serve RPC
	var rpcConfig rpc.Config
	viperUtil.MustUnmarshalKey("rpc", &rpcConfig)
	go rpc.MustServe(rpcConfig, store)
	logrus.WithField("config", fmt.Sprintf("%+v", rpcConfig)).Info("RPC started")

	// TODO graceful shutdown
	select {}
}
