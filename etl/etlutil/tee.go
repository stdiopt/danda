package etlutil

import (
	"context"

	"github.com/stdiopt/danda/etl"
)

// Tee returns a new iterator that will return all the values from it
// and also pass them to the iterator in the given function.
func Tee(it Iter, fn func(it Iter)) Iter {
	return etl.MakeGen(etl.Gen[any]{
		Run: func(ctx context.Context, yield etl.Y[any]) error {
			in := make(chan any)
			defer close(in)
			go func() {
				it2 := etl.Chan(in)
				fn(it2)
			}()

			return etl.ConsumeContext(ctx, it, func(v any) error {
				in <- v
				return yield(v)
			})
		},
		Close: it.Close,
	})
}
