package types

import "github.com/DmitriyVTitov/size"

// Sizable is the interface implemented by types that support to compute memory size.
type Sizable interface {
	Size() int // memory size in bytes
}

// Sized wraps a value with its precomputed memory size in bytes.
type Sized[T any] struct {
	Value T
	Size  int
}

// NewSized constructs a Sized wrapper around a value with an optional precomputed size in bytes.
//
// If `bytes` not specified, it will be computed automatically via `Sizable` interface or reflection.
func NewSized[T any](value T, bytes ...int) Sized[T] {
	calSize := 0
	if len(bytes) > 0 {
		calSize = bytes[0]
	} else if sizable, ok := any(value).(Sizable); ok {
		calSize = sizable.Size()
	} else {
		calSize = size.Of(value)
	}

	return Sized[T]{
		Value: value,
		Size:  calSize,
	}
}
