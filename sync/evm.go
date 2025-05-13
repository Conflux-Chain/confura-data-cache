package sync

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	_ EthExtractor = (*extract.EthExtractor)(nil)
	_ EthStore     = (*leveldb.Store)(nil)
)

type EthExtractor interface {
	Start(context.Context, *extract.EthMemoryBoundedChannel)
}

type EthExtractorFactory func(conf extract.EthConfig) (EthExtractor, error)

type EthStore interface {
	NextBlockNumber() uint64
	Write(...types.EthBlockData) error
}

type EthSyncer struct {
	EthConfig
	store              EthStore
	finalizedExtractor EthExtractor
	writeBuffer        []types.EthBlockData
}

func NewEthSyncer(conf EthConfig, store *leveldb.Store) (*EthSyncer, error) {
	extractorFactory := func(conf extract.EthConfig) (EthExtractor, error) {
		return extract.NewEthExtractor(conf)
	}
	return newEthSyncer(conf, store, extractorFactory)
}

func newEthSyncer(conf EthConfig, store EthStore, extractorFactory EthExtractorFactory) (*EthSyncer, error) {
	// Create finalized extractor
	extractConf := conf.Extract
	extractConf.StartBlockNumber = ethTypes.BlockNumber(store.NextBlockNumber())
	extractConf.TargetBlockNumber = ethTypes.FinalizedBlockNumber
	finalizedExtractor, err := extractorFactory(extractConf)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create finalized extractor")
	}

	return &EthSyncer{
		EthConfig:          conf,
		store:              store,
		finalizedExtractor: finalizedExtractor,
		writeBuffer:        make([]types.EthBlockData, 0, conf.BatchSize),
	}, nil
}

func (s *EthSyncer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	dataChan := extract.NewEthMemoryBoundedChannel(s.Extract.MaxMemoryUsageBytes)
	defer dataChan.Close()

	go s.finalizedExtractor.Start(ctx, dataChan)
	defer s.flushWriteBuffer()

	for {
		select {
		case <-ctx.Done():
			return
		case res, ok := <-dataChan.RChan():
			if ok {
				s.processFinalized(res)
			}
		}
	}
}

func (s *EthSyncer) processFinalized(result *extract.EthRevertableBlockData) {
	// Finalized block data will never be reorg-ed

	s.writeBuffer = append(s.writeBuffer, *result.BlockData)
	if s.BatchSize > 0 && len(s.writeBuffer) < s.BatchSize {
		return
	}

	for {
		err := s.flushWriteBuffer()
		if err == nil {
			break
		}

		logrus.WithFields(logrus.Fields{
			"blockNumber": result.BlockData.Block.Number.Uint64(),
			"blockHash":   result.BlockData.Block.Hash,
		}).WithError(err).Error("Eth finalized syncer failed to write batch buffer")
		time.Sleep(time.Second)
	}
}

func (s *EthSyncer) flushWriteBuffer() error {
	if len(s.writeBuffer) == 0 {
		return nil
	}

	err := s.store.Write(s.writeBuffer...)
	if err != nil {
		return err
	}
	s.writeBuffer = s.writeBuffer[:0]
	return nil
}
