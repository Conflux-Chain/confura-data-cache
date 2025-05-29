package sync

import (
	"time"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/go-conflux-util/health"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

type PersistenceConfig struct {
	BatchSize     int           `default:"10"` // Number of blocks to write in a single batch
	BatchInterval time.Duration `default:"5s"` // Interval to write a batch
}

type EthConfig struct {
	Persistence PersistenceConfig         // Configurations for persistence
	Extract     extract.EthConfig         // Configurations for the extractor
	Health      health.TimedCounterConfig // health configurations
}

func MustNewEthSyncerFromViper(store *leveldb.Store) *EthSyncer {
	var conf EthConfig
	viper.MustUnmarshalKey("sync", &conf)

	syncer, err := NewEthSyncer(conf, store)
	if err != nil {
		logrus.WithField("config", conf).WithError(err).Fatal("Failed to create EthSyncer")
	}

	return syncer
}
