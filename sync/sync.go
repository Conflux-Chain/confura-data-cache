package sync

import (
	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
)

func MustNewEthSyncerFromViper(cache *nearhead.EthCache, store *leveldb.Store) *EthSyncer {
	var conf extract.EthConfig
	viper.MustUnmarshalKey("extractor", &conf)

	syncer, err := NewEthSyncer(conf, cache, store)
	if err != nil {
		logrus.WithField("config", conf).WithError(err).Fatal("Failed to create EthSyncer")
	}

	return syncer
}
