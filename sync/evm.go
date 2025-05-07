package sync

import (
	"context"
	"sync"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/extract"
	"github.com/Conflux-Chain/confura-data-cache/nearhead"
	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
	ethTypes "github.com/openweb3/web3go/types"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	_ EthExtractor = (*extract.EthExtractor)(nil)
	_ EthCache     = (*nearhead.EthCache)(nil)
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

type EthCache interface {
	Put(*types.Sized[*types.EthBlockData]) error
	Pop(bn uint64) bool
}

type EthSyncer struct {
	extract.EthConfig
	store                                 EthStore
	cache                                 EthCache
	finalizedExtractor, nearheadExtractor EthExtractor
}

func NewEthSyncer(conf extract.EthConfig, cache *nearhead.EthCache, store *leveldb.Store) (*EthSyncer, error) {
	extractorFactory := func(conf extract.EthConfig) (EthExtractor, error) {
		return extract.NewEthExtractor(conf)
	}
	return newEthSyncer(conf, cache, store, extractorFactory)
}

func newEthSyncer(
	conf extract.EthConfig, cache EthCache, store EthStore, extractorFactory EthExtractorFactory,
) (*EthSyncer, error) {
	conf.StartBlockNumber = ethTypes.FinalizedBlockNumber
	conf.TargetBlockNumber = ethTypes.LatestBlockNumber
	nearheadExtractor, err := extractorFactory(conf)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create nearhead extractor")
	}

	conf.StartBlockNumber = ethTypes.BlockNumber(store.NextBlockNumber())
	conf.TargetBlockNumber = ethTypes.FinalizedBlockNumber
	finalizedExtractor, err := extractorFactory(conf)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to create finalized extractor")
	}

	return &EthSyncer{
		EthConfig:          conf,
		store:              store,
		cache:              cache,
		finalizedExtractor: finalizedExtractor,
		nearheadExtractor:  nearheadExtractor,
	}, nil
}

func (s *EthSyncer) Run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(2)

	go s.startNearHeadSync(ctx, wg)
	s.startFinalizedSync(ctx, wg)
}

func (s *EthSyncer) startNearHeadSync(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	dataChan := extract.NewEthMemoryBoundedChannel(s.MaxMemoryUsageBytes)
	defer dataChan.Close()

	go s.nearheadExtractor.Start(ctx, dataChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			result, ok, err := dataChan.TryReceive()
			if err != nil { // channel closed?
				return
			}
			if ok {
				s.processNearhead(result)
			} else {
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (s *EthSyncer) startFinalizedSync(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	dataChan := extract.NewEthMemoryBoundedChannel(s.MaxMemoryUsageBytes)
	defer dataChan.Close()

	go s.finalizedExtractor.Start(ctx, dataChan)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			result, ok, err := dataChan.TryReceive()
			if err != nil { // channel closed?
				return
			}
			if ok {
				s.processFinalized(result)
			} else {
				time.Sleep(time.Millisecond)
			}
		}
	}
}

func (s *EthSyncer) processNearhead(result *extract.EthRevertableBlockData) {
	if bn := result.ReorgHeight; bn != nil {
		popped := s.cache.Pop(*bn)
		logrus.WithFields(logrus.Fields{
			"reorgHeight": *bn,
			"popped":      popped,
		}).Info("Eth nearhead syncer reorg handled")
		return
	}

	sizedBlockData := types.NewSized(result.BlockData)
	for {
		err := s.cache.Put(&sizedBlockData)
		if err == nil {
			break
		}

		logrus.WithFields(logrus.Fields{
			"blockNumber": result.BlockData.Block.Number.Uint64(),
			"blockHash":   result.BlockData.Block.Hash,
		}).WithError(err).Error("Eth nearhead syncer failed to cache block data")
		time.Sleep(time.Second)
	}
}

func (s *EthSyncer) processFinalized(result *extract.EthRevertableBlockData) {
	// Finalized block data will never be reorg-ed

	blockData := result.BlockData
	for {
		err := s.store.Write(*blockData)
		if err == nil {
			break
		}

		logrus.WithFields(logrus.Fields{
			"blockNumber": blockData.Block.Number.Uint64(),
			"blockHash":   blockData.Block.Hash,
		}).WithError(err).Error("Eth finalized syncer failed to store block data")
		time.Sleep(time.Second)
	}
}
