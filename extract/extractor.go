package extract

import (
	"time"

	"github.com/Conflux-Chain/go-conflux-util/parallel"
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
	// Concurrency options used for catching up to the latest finalized block.
	parallel.SerialOption

	// TargetBlockNumber is the block number the extractor is catching up to during synchronization.
	TargetBlockNumber T

	// Blockchain RPC endpoints to connect to, which will be used to fetch block data.
	RpcEndpoint string

	// Optional starting block number for sync.
	// If not set (i.e., 0), sync will start from the earliest block.
	StartBlockNumber T

	// Maximum memory (in bytes) the extractor is allowed to use.
	// If memory usage exceeds this limit, syncing will be paused.
	// Default is 256MB (268,435,456 bytes).
	MaxMemoryUsageBytes int `default:"268435456"`

	// PollInterval specifies how often to poll for new blocks during normal, steady-state operation.
	PollInterval time.Duration `default:"1s"`
}

// EthConfig is a Config specialization for Ethereum-compatible blockchains.
type EthConfig Config[rpc.BlockNumber]
