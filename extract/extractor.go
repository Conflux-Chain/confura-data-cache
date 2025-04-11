package extract

import (
	"context"
	"time"

	"github.com/Conflux-Chain/confura-data-cache/types"
	"github.com/pkg/errors"
)

var (
	// Ensure EvmExtractor implements the Extractor interface
	_ Extractor[types.EthBlockData] = (*EvmExtractor)(nil)

	// ErrInconsistentChainData indicates that the blockchain data fetched is inconsistent.
	// Consumers of this extractor should treat this as a fatal or blocking error for the current data set.
	ErrInconsistentChainData = errors.New("inconsistent chain data")
)

// NewInconsistentChainDataError wraps ErrInconsistentChainData with a specific reason.
// This allows detailed error reporting while preserving the ability to match with errors.Is().
func NewInconsistentChainDataError(detail string) error {
	return errors.WithMessage(ErrInconsistentChainData, detail)
}

type Config struct {
	// List of blockchain RPC endpoints to connect to.
	// These will be used to fetch block data.
	RpcEndpoints []string

	// Optional starting block number for sync.
	// If not set (i.e., 0), sync will start from the earliest block.
	StartBlockNumber uint64

	// Maximum number of blocks to sync per batch.
	// Useful for optimizing throughput during batch processing.
	MaxBatchSize uint64 `default:"10"`

	// Size of the internal buffer (in number of blocks) to store fetched block data before processing.
	// Acts as a channel queue size between data fetcher and processor.
	// Helps with backpressure and batching. Default is 200.
	ResultBufferSize int `default:"200"`

	// Maximum memory (in bytes) the extractor is allowed to use.
	// If memory usage exceeds this limit, syncing will be paused or throttled.
	// Default is 1GB (1073741824 bytes).
	MaxMemoryUsageBytes uint64 `default:"1073741824"`
}

// ExtractOptions configures the behavior of the blockchain data extractor,
// particularly its synchronization and polling strategy.
type ExtractOptions struct {
	// AlignBlockTag defines the block tag (e.g., "latest", "finalized") that the
	// extractor synchronizes with. Reaching this tag marks the transition from
	// the "catch-up" phase to the "aligned" phase.
	AlignBlockTag string `default:"latest"`

	// AlignedPollInterval specifies how often to poll for new blocks once the
	// extractor is synchronized (i.e., has reached the AlignBlockTag).
	// This is the polling rate during normal, steady-state operation.
	AlignedPollInterval time.Duration `default:"1s"`

	// CatchupPollInterval specifies how often to poll for new blocks while the
	// extractor is behind the AlignBlockTag. This faster polling rate helps
	// expedite the initial synchronization process.
	CatchupPollInterval time.Duration `default:"1ms"`
}

// Extractor is an interface for extracting data from blockchain data sources.
type Extractor[T any] interface {
	// Subscribe subscribes to the data source and returns a channel to receive the data.
	Subscribe(context.Context, ...ExtractOptions) (<-chan T, error)

	// Unsubscribe unsubscribes from the data source.
	Unsubscribe(ctx context.Context) error
}
