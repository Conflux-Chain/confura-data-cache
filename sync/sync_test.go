package sync

import (
	"os"
	"testing"

	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	viperutil "github.com/Conflux-Chain/go-conflux-util/viper"
	"github.com/sirupsen/logrus"
	viper "github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestMustNewEthSyncerFromViper(t *testing.T) {
	t.Parallel()

	t.Run("Success", func(t *testing.T) {
		viperutil.MustInit("CDC", "../config.yml")
		defer viper.Reset()

		os.Setenv("CDC_SYNC_BATCHSIZE", "1")
		defer os.Setenv("CDC_SYNC_BATCHSIZE", "")
		os.Setenv("CDC_SYNC_EXTRACT_RPCENDPOINT", "http://localhost:8545")
		defer os.Setenv("CDC_SYNC_EXTRACT_RPCENDPOINT", "")

		syncer := MustNewEthSyncerFromViper(&leveldb.Store{})
		assert.NotNil(t, syncer)
		assert.Equal(t, 1, syncer.BatchSize)
	})

	t.Run("Error", func(t *testing.T) {
		defer func() {
			assert.Equal(t, "exit", recover())
		}()

		logrus.StandardLogger().ExitFunc = func(code int) {
			panic("exit") // force flow to break
		}

		MustNewEthSyncerFromViper(&leveldb.Store{})
	})
}
