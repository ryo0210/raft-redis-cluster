package store

import (
	"context"
	"io"

	"github.com/bootjp/go-kvlib/store"
)

type Txn = store.Txn

type Store interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Put(ctx context.Context, key []byte, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Exists(ctx context.Context, key []byte) (bool, error)
	Snapshot() (io.ReadWriter, error)
	Restore(buf io.Reader) error
	Txn(ctx context.Context, f func(ctx context.Context, txn Txn) error) error
	Close() error
}

var ErrKeyNotFound = store.ErrKeyNotFound

func NewMemoryStore() Store {
	return store.NewMemoryStore()
}
