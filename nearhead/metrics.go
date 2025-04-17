package nearhead

import (
	metricsUtil "github.com/Conflux-Chain/go-conflux-util/metrics"
	"github.com/ethereum/go-ethereum/metrics"
)

type Metrics struct{}

func (m *Metrics) Latest() metrics.Gauge {
	return metricsUtil.GetOrRegisterGauge("nearhead/cache/latest")
}

func (m *Metrics) CurrentSize() metrics.Gauge {
	return metricsUtil.GetOrRegisterGauge("nearhead/cache/size")
}

func (m *Metrics) Put() metrics.Timer {
	return metricsUtil.GetOrRegisterTimer("nearhead/cache/put")
}

func (m *Metrics) Hit(method string) metricsUtil.Percentage {
	return metricsUtil.GetOrRegisterTimeWindowPercentageDefault(0, "nearhead/cache/hit/%v", method)
}
