package etlutil

import "github.com/stdiopt/danda/etl"

// Tee returns a new iterator that will return all the values from it
// and also pass them to the iterator in the given function.
func Tee(it Iter, fn func(it Iter)) Iter {
	return etl.MakeGen(etl.Gen[any]{
		Run: func(yield etl.Y[any]) error {
			in := make(chan any)
			defer close(in)
			go func() {
				it2 := etl.Chan(in)
				fn(it2)
			}()

			return etl.Consume(it, func(v any) error {
				in <- v
				return yield(v)
			})
		},
		Close: it.Close,
	})
}
