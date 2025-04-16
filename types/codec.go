package types

import (
	"encoding/json"
)

// Lazy wraps encoded data and decode on demand for CPU saving.
type Lazy[T any] struct {
	encoded []byte
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
