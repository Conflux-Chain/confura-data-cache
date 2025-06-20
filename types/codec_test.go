package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

type Student struct {
	Name string
	Age  uint8
}

func assertLazyByUnmarshal[T any](t *testing.T, s1 T) {
	encoded1, err := json.Marshal(s1)
	assert.Nil(t, err)

	var s2 Lazy[T]
	json.Unmarshal(encoded1, &s2)

	assert.Equal(t, s1, s2.MustLoad())

	encoded2, err := json.Marshal(s2)
	assert.Nil(t, err)
	assert.Equal(t, encoded1, encoded2)
}

func TestLazyByUnmarshal(t *testing.T) {
	// struct
	assertLazyByUnmarshal(t, Student{Name: "Wendy", Age: 18})

	// pointer
	assertLazyByUnmarshal(t, &Student{Name: "Wendy", Age: 18})

	// pointer nil
	assertLazyByUnmarshal(t, (*Student)(nil))

	// slice
	assertLazyByUnmarshal(t, []Student{
		{Name: "Wendy", Age: 18},
		{Name: "Cendy", Age: 17},
	})

	// slice empty
	assertLazyByUnmarshal(t, []Student{})

	// slice nil
	assertLazyByUnmarshal(t, []Student(nil))
}

func assertLazyByDefault[T any](t *testing.T, ptr bool) {
	var s1 T
	encoded1, err := json.Marshal(s1)
	assert.Nil(t, err)

	var s2 Lazy[T]
	assert.True(t, s2.IsEmptyOrNull())
	assert.Equal(t, s1, s2.MustLoad())

	encoded2, err := json.Marshal(s2)
	assert.Nil(t, err)
	assert.Equal(t, encoded1, encoded2)

	var s3 Lazy[T]
	assert.Nil(t, json.Unmarshal(encoded1, &s3))
	if ptr {
		assert.True(t, s3.IsEmptyOrNull())
	} else {
		assert.False(t, s3.IsEmptyOrNull())
	}
}

func TestLazyByDefault(t *testing.T) {
	// struct
	assertLazyByDefault[Student](t, false)

	// pointer
	assertLazyByDefault[*Student](t, true)

	// slice
	assertLazyByDefault[[]Student](t, true)
}

func TestLazyFromStruct(t *testing.T) {
	s1 := Student{Name: "Wendy", Age: 18}

	lazy, err := NewLazy(s1)
	assert.Nil(t, err)
	assert.Equal(t, s1, lazy.MustLoad())
}
