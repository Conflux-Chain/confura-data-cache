package sync

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	"github.com/Conflux-Chain/confura-data-cache/store"
	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/Conflux-Chain/go-conflux-util/health"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	_ EthExtractor = (*extract.EthExtractor)(nil)
	_ EthCache     = (*nearhead.EthCache)(nil)
)

type EthExtractor interface {
	Start(context.Context, *extract.EthMemoryBoundedChannel)
}

type EthExtractorFactory func(conf extract.EthConfig) (EthExtractor, error)

type EthCache interface {
	Pop(uint64) bool
	Put(sized *types.Sized[*types.EthBlockData]) error
}

type EthSyncer struct {
	EthConfig
	store              store.Writable
	finalizedExtractor EthExtractor
	lastFlushAt        time.Time
	writeBuffer        []types.EthBlockData
	health             *health.TimedCounter
}

func NewEthSyncer(conf EthConfig, store store.Writable) (*EthSyncer, error) {
	extractorFactory := func(conf extract.EthConfig) (EthExtractor, error) {
		return extract.NewEthExtractor(conf)
	}
	return newEthSyncer(conf, store, extractorFactory)
}

func newEthSyncer(conf EthConfig, store store.Writable, extractorFactory EthExtractorFactory) (*EthSyncer, error) {
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
		lastFlushAt:        time.Now(),
		writeBuffer:        make([]types.EthBlockData, 0, conf.Persistence.BatchSize),
		health:             health.NewTimedCounter(conf.Health),
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
		case res := <-dataChan.RChan():
			if res != nil {
				s.processFinalized(res)
			}
		}
	}
}

func (s *EthSyncer) processFinalized(result *extract.EthRevertableBlockData) {
	// Finalized block data will never be reorg-ed

	s.writeBuffer = append(s.writeBuffer, *result.BlockData)
	if len(s.writeBuffer) < s.Persistence.BatchSize && time.Since(s.lastFlushAt) < s.Persistence.BatchInterval {
		// If the buffer is not full and the interval is not reached, just return
		return
	}

	for {
		err := s.flushWriteBuffer()

		recovered, unhealthy, unrecovered, elapsed := s.health.OnError(err)
		if recovered {
			logrus.WithField("elapsed", elapsed).Warn("Eth finalized syncer recovered")
		} else if unhealthy {
			logrus.WithError(err).WithField("elapsed", elapsed).Warn("Eth finalized syncer failed to write batch buffer")
		} else if unrecovered {
			logrus.WithError(err).WithField("elapsed", elapsed).Warn("Eth finalized syncer failed to write batch buffer for a long time")
		}

		if err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

func (s *EthSyncer) flushWriteBuffer() error {
	s.lastFlushAt = time.Now()

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

// EthNearHeadSyncer syncs the nearhead block data into memory cache.
type EthNearHeadSyncer struct {
	EthConfig
	cache             EthCache
	health            *health.TimedCounter
	nearHeadExtractor EthExtractor
}

func NewEthNearHeadSyncer(conf EthConfig, cache *nearhead.EthCache) (*EthNearHeadSyncer, error) {
	extractorFactory := func(conf extract.EthConfig) (EthExtractor, error) {
		return extract.NewEthExtractor(conf)
	}
	return newEthNearHeadSyncer(conf, cache, extractorFactory)
}

func newEthNearHeadSyncer(conf EthConfig, cache EthCache, extractorFactory EthExtractorFactory) (*EthNearHeadSyncer, error) {
	// Create nearhead extractor
	extractConf := conf.Extract
	extractConf.StartBlockNumber = ethTypes.FinalizedBlockNumber
	extractConf.TargetBlockNumber = ethTypes.LatestBlockNumber
	nearHeadExtractor, err := extractorFactory(extractConf)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create near head extractor")
	}

	return &EthNearHeadSyncer{
		EthConfig:         conf,
		cache:             cache,
		nearHeadExtractor: nearHeadExtractor,
		health:            health.NewTimedCounter(conf.Health),
	}, nil
}

func (s *EthNearHeadSyncer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	dataChan := extract.NewEthMemoryBoundedChannel(s.Extract.MaxMemoryUsageBytes)
	defer dataChan.Close()

	go s.nearHeadExtractor.Start(ctx, dataChan)

	for {
		select {
		case <-ctx.Done():
			return
		case res := <-dataChan.RChan():
			if res != nil {
				s.processNearHead(res)
			}
		}
	}
}

func (s *EthNearHeadSyncer) processNearHead(result *extract.EthRevertableBlockData) {
	// Pop cache for reorg
	if result.ReorgHeight != nil {
		s.cache.Pop(*result.ReorgHeight)
		return
	}

	// Put the block data into cache
	sizedBlockData := types.NewSized(result.BlockData)
	for {
		err := s.cache.Put(&sizedBlockData)
		recovered, unhealthy, unrecovered, elapsed := s.health.OnError(err)
		if recovered {
			logrus.WithField("elapsed", elapsed).Warn("Eth near head syncer recovered")
		} else if unhealthy {
			logrus.WithError(err).WithField("elapsed", elapsed).Warn("Eth near head syncer failed to write cache")
		} else if unrecovered {
			logrus.WithError(err).WithField("elapsed", elapsed).Warn("Eth near head syncer failed to write cache for a long time")
		}

		if err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}
