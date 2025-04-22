package extract

import (
	"time"

	"github.com/openweb3/go-rpc-provider"
	"github.com/pkg/errors"
)

var (
	// ErrInconsistentChainData indicates that the blockchain data fetched is inconsistent.
	// Consumers of this extractor should treat this as a fatal or blocking error for the current data set.
	ErrInconsistentChainData = errors.New("inconsistent chain data")
)

// NewInconsistentChainDataError wraps ErrInconsistentChainData with a specific reason.
// This allows detailed error reporting while preserving the ability to match with errors.Is().
func NewInconsistentChainDataError(detail string) error {
	return errors.WithMessage(ErrInconsistentChainData, detail)
}

type Config[T any] struct {
	// TargetBlockNumber is the block number the extractor is catching up to during synchronization.
	TargetBlockNumber T

	// Blockchain RPC endpoints to connect to, which will be used to fetch block data.
	RpcEndpoint string

	// Optional starting block number for sync.
	// If not set (i.e., 0), sync will start from the earliest block.
	StartBlockNumber uint64

	// Size of the internal buffer (in number of blocks) to store fetched block data before processing.
	// Acts as a channel queue size between data producer and consumer.
	BufferSize int `default:"200"`

	// Maximum memory (in bytes) the extractor is allowed to use.
	// If memory usage exceeds this limit, syncing will be paused.
	// Default is 256MB (268,435,456 bytes).
	MaxMemoryUsageBytes uint64 `default:"268435456"`

	// PollInterval specifies how often to poll for new blocks during normal, steady-state operation.
	PollInterval time.Duration `default:"1s"`

	// Number of concurrent goroutines used for catching up to the latest finalized block.
	// By default, runtime.GOMAXPROCS(0) is used.
	Concurrency int
}

// EthConfig is a Config specialization for Ethereum-compatible blockchains.
type EthConfig Config[rpc.BlockNumber]
