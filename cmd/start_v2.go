package cmd

import (
	"context"
	"sync"

	"github.com/Conflux-Chain/confura-data-cache/store"
	dataSync "github.com/Conflux-Chain/confura-data-cache/sync"
	"github.com/Conflux-Chain/go-conflux-util/cmd"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/spf13/cobra"
)

var startV2Cmd = &cobra.Command{
	Use:   "startv2",
	Short: "Start data cache service to sync data from fullnode and provides RPC and gRPC service",
	Run:   startv2,
}

func init() {
	rootCmd.AddCommand(startCmd)
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

func mustStartDataSync(ctx context.Context, wg *sync.WaitGroup, store store.Writable) {
	var config dataSync.Config
	viper.MustUnmarshalKey("sync", &config)

	worker, err := dataSync.NewWorker(config, store)
	cmd.FatalIfErr(err, "Failed to create data sync worker")

	wg.Add(1)
	go worker.Run(ctx, wg)
}
