package etlutil

import (
	"context"
	"fmt"

	"github.com/stdiopt/danda/etl"
)

// Slice chunks values
//
// Deprecated: use etl.Chunk instead
func Slice[T any](it Iter, n int) Iter {
	return etl.Chunk[T](it, n)
}

// Unslice receives a slice of type T and yields each value.
func Unslice[T any](it Iter) Iter {
	var cur []T
	curi := 0
	return etl.MakeIter(etl.Custom[T]{
		Next: func(ctx context.Context) (T, error) {
			var z T
			if curi >= len(cur) {
				vv, err := it.Next(ctx)
				if err != nil {
					return z, err
				}

				v, ok := vv.([]T)
				if !ok {
					return z, fmt.Errorf("expected slice of type %T, got %T", z, vv)
				}
				cur = v
				curi = 0
			}
			val := cur[curi]
			curi++
			return val, nil
		},
		Close: it.Close,
	})
}
