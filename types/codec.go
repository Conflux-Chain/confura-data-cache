package types

import (
	"encoding/json"

	"github.com/pkg/errors"
)

// Lazy wraps encoded data and decode on demand for CPU saving.
type Lazy[T any] struct {
	encoded []byte
}

func NewLazy[T any](v T) (Lazy[T], error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return Lazy[T]{}, errors.WithMessage(err, "Failed to json marshal value")
	}

	return Lazy[T]{encoded}, nil
}

// MarshalJSON implements the json.Marshaler interface.
func (lazy Lazy[T]) MarshalJSON() ([]byte, error) {
	if len(lazy.encoded) > 0 {
		return lazy.encoded, nil
	}

	// marshal default value if not constructed from JSON
	var val T
	return json.Marshal(val)
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (lazy *Lazy[T]) UnmarshalJSON(v []byte) error {
	lazy.encoded = v
	return nil
}

// Load returns the decoded data.
func (lazy *Lazy[T]) Load() (val T, err error) {
	// return default value if not constructed from JSON
	if len(lazy.encoded) == 0 {
		return
	}

	err = json.Unmarshal(lazy.encoded, &val)
	return
}

// MustLoad should always return the decoded data without any decode error.
func (lazy *Lazy[T]) MustLoad() T {
	val, err := lazy.Load()
	if err != nil {
		panic(err)
	}

	return val
}

// IsEmpty indicates if this lazy object is empty.
func (lazy *Lazy[T]) IsEmpty() bool {
	return len(lazy.encoded) == 0
}
