package leveldb

import (
	metricsUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/metrics"
)

type Metrics struct{}

func (m *Metrics) Latest() metrics.Gauge {
	return metricsUtil.GetOrRegisterGauge("store/leveldb/latest")
}

func (m *Metrics) Write() metrics.Timer {
	return metricsUtil.GetOrRegisterTimer("store/leveldb/write")
}

func (m *Metrics) NumTxs() metrics.Histogram {
	return metricsUtil.GetOrRegisterHistogram("store/leveldb/num/txs")
}

func (m *Metrics) NumTraces() metrics.Histogram {
	return metricsUtil.GetOrRegisterHistogram("store/leveldb/num/traces")
}
