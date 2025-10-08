// Package etljson provides iterators to handle json.
package etljson

import (
	"context"
	"encoding/json"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
)

// Iter alias to iter.Iter
type Iter = etl.Iter

// Decode returns an iterator that consumes bytes from a source iterator,
// unmarshal, and yield data of type T
func Decode[T any](it Iter) Iter {
	dec := json.NewDecoder(etlio.AsReader(it))
	return etl.MakeIter(etl.Custom[T]{
		Next: func(context.Context) (T, error) {
			var v T
			err := dec.Decode(&v)
			return v, err
		},
		Close: it.Close,
	})
}

// Encode encodes encoming data from it and returns an iterator that yields []byte.
func Encode(it Iter) Iter {
	return etl.MakeIter(etl.Custom[[]byte]{
		Next: func(ctx context.Context) ([]byte, error) {
			v, err := it.Next(ctx)
			if err != nil {
				return nil, err
			}

			return json.Marshal(v)
		},
	})
}
