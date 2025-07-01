package extract

import (
	metricsUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/rcrowley/go-metrics"
)

var (
	ethMetrics = Metrics{chain: "eth"}
)

type Metrics struct {
	chain string // block chain name
}

// Qps returns a timer for measuring the number of extractions per second.
func (m *Metrics) Qps() metrics.Timer {
	return metricsUtil.GetOrRegisterTimer("extract/%v/once", m.chain)
}

// Availability returns a percentage for measuring the availability of query requests.
func (m *Metrics) Availability() metricsUtil.Percentage {
	return metricsUtil.GetOrRegisterTimeWindowPercentageDefault("extract/%v/query/availability", m.chain)
}

// Latency returns a histogram for measuring the latency of query requests.
func (m *Metrics) Latency(success bool) metrics.Histogram {
	if success {
		return metricsUtil.GetOrRegisterHistogram("extract/%v/query/latency/success", m.chain)
	}
	return metricsUtil.GetOrRegisterHistogram("extract/%v/query/latency/failure", m.chain)
}

// DataSize returns a histogram for measuring the size of queried block data.
func (m *Metrics) DataSize() metrics.Histogram {
	return metricsUtil.GetOrRegisterHistogram("extract/%v/query/data/size", m.chain)
}
