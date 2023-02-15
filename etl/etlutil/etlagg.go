package etlutil

import (
	"context"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/util/dagg"
)

// Group groups data and produces a Row with the data.
func Group[T any](it Iter, gfn dagg.GroupFn[T], opts ...dagg.OptFn[T]) Iter {
	var o *dagg.Agg[T]
	return etl.MakeIter(etl.Custom[Row]{
		Next: func(ctx context.Context) (Row, error) {
			if o == nil {
				o = &dagg.Agg[T]{}
				o.GroupBy(gfn)
				for _, fn := range opts {
					fn(o)
				}
				if err := etl.ConsumeContext(ctx, it, o.Add); err != nil {
					return nil, err
				}
			}
			return o.Next()
		},
		Close: it.Close,
	})
}

// GroupByFuncE accepts a function that expects a V type to group iterations
// and allows an error to be returned.
func GroupByFuncE[T, V any](fn func(T) (V, error)) dagg.GroupFn[T] {
	return func(v T) (any, error) {
		return fn(v)
	}
}

// GroupByFunc accepts a func that will group iterations.
func GroupByFunc[T, V any](fn func(T) V) dagg.GroupFn[T] {
	return func(v T) (any, error) {
		return fn(v), nil
	}
}

// Reduce adds a reduce func to aggregator
func Reduce[Ta, T any](name string, fn func(Ta, T) Ta, ffn ...func(Ta) any) dagg.OptFn[T] {
	reducefn := func(a any, v T) any {
		acc, _ := a.(Ta)
		return fn(acc, v)
	}
	var finalfn func(any) any
	if len(ffn) > 0 {
		finalfn = func(a any) any {
			acc, _ := a.(Ta)
			return ffn[0](acc)
		}
	}

	return func(a *dagg.Agg[T]) {
		a.Reduce(name, reducefn, finalfn)
	}
}
