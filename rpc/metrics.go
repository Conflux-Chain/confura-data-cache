package rpc

import metricsUtil "github.com/Conflux-Chain/go-conflux-util/metrics"

var metrics Metrics

type Metrics struct{}

func (m *Metrics) GetBlockIsFull() metricsUtil.Percentage {
	return metricsUtil.GetOrRegisterTimeWindowPercentageDefault(0, "rpc/api/getBlock/full")
}

func (m *Metrics) GetBlockHitCache(isFull bool) metricsUtil.Percentage {
	if isFull {
		return metricsUtil.GetOrRegisterTimeWindowPercentageDefault(0, "rpc/api/getBlock/hit/cache/block")
	}

	return metricsUtil.GetOrRegisterTimeWindowPercentageDefault(0, "rpc/api/getBlock/hit/cache/summary")
}
