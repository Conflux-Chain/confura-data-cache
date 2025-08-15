package extract

import (
	"testing"

	"github.com/ethereum/go-ethereum/metrics"
	"github.com/stretchr/testify/assert"
)

func TestMetrics(t *testing.T) {
	// enable metrics
	metrics.Enable()

	// QPS
	timer := ethMetrics.Qps()
	assert.NotNil(t, timer)

	// Availability
	availability := ethMetrics.Availability()
	assert.NotNil(t, availability)

	//  Latency
	latency := ethMetrics.Latency(true)
	assert.NotNil(t, latency)

	latency = ethMetrics.Latency(false)
	assert.NotNil(t, latency)

	// DataSize
	dataSize := ethMetrics.DataSize()
	assert.NotNil(t, dataSize)
}
