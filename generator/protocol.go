package generator

import (
	"errors"
	"io"
)

var ErrKeyDoesNotExist = errors.New("key does not exist")

// A Generator is a generic id provider.
type Generator interface {
	io.Closer
	EnableKeys([]string) error
	Next(string) (int64, error)
	Current(string) (int64, error)
}
