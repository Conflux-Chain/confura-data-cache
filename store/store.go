package store

import (
	"io"

	"github.com/Conflux-Chain/confura-data-cache/store/leveldb"
	"github.com/Conflux-Chain/confura-data-cache/types"
)

var (
	_ Store[types.EthBlockData] = (*leveldb.Store[types.EthBlockData])(nil)
	_ Store[types.CfxBlockData] = (*leveldb.Store[types.CfxBlockData])(nil)
)

// Store interface to manage block data in persistent storage.
type Store[T types.BlockData] interface {
	io.Closer

	Write(data T) error
}
