package store

import (
	"io"

	"github.com/Conflux-Chain/confura-data-cache/types"
)

// Store interface to manage block data in persistent storage.
type Store[T types.BlockData] interface {
	io.Closer

	Write(data T) error
}
