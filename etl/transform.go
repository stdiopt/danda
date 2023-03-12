package etl

import (
	"context"
	"fmt"
)

func FlatMap[Ti, To any](it Iter, fn func(Ti) []To) Iter {
	var t []To
	return MakeIter(Custom[To]{
		Next: func(ctx context.Context) (To, error) {
			if len(t) == 0 {
				var z To
				vv, err := it.Next(ctx)
				if err != nil {
					return z, err
				}
				v, ok := vv.(Ti)
				if !ok {
					return z, fmt.Errorf("iter.Map: type mismatch: %T", vv)
				}
				t = fn(v)
			}

			v := t[0]
			t = t[1:]
			return v, nil
		},
		Close: it.Close,
	})
}

// Map returns an interator that transforms the values of the source
// iterator using the func fn.
func Map[Ti, To any](it Iter, fn func(Ti) To) Iter {
	return MakeIter(Custom[To]{
		Next: func(ctx context.Context) (To, error) {
			var z To
			vv, err := it.Next(ctx)
			if err != nil {
				return z, err
			}
			v, ok := vv.(Ti)
			if !ok {
				return z, fmt.Errorf("iter.Map: type mismatch: %T", vv)
			}

			return fn(v), nil
		},
		Close: it.Close,
	})
}

// Map returns an interator that transforms the values of the source
// iterator using the func fn.
func MapE[Ti, To any](it Iter, fn func(Ti) (To, error)) Iter {
	return MakeIter(Custom[To]{
		Next: func(ctx context.Context) (To, error) {
			var z To
			vv, err := it.Next(ctx)
			if err != nil {
				return z, err
			}
			v, ok := vv.(Ti)
			if !ok {
				return z, fmt.Errorf("iter.Map: type mismatch: %T", vv)
			}

			return fn(v)
		},
		Close: it.Close,
	})
}

type FilterFunc[T any] func(T) bool

// Filter returns an iterator that filters the values of the source given the func fn
// if fn returns true the value is passed through
func Filter[T any](it Iter, fn FilterFunc[T]) Iter {
	return MakeIter(Custom[T]{
		Next: func(ctx context.Context) (T, error) {
			var z T
			for {
				vv, err := it.Next(ctx)
				if err != nil {
					return z, err
				}
				v, ok := vv.(T)
				if !ok {
					return z, fmt.Errorf("iter.Filter: type mismatch: %T", vv)
				}
				if fn(v) {
					return v, nil
				}
			}
		},
		Close: it.Close,
	})
}

// Yield returns an iterator that for each consumed value calls a fn passing an
// yielder,
func Yield[Ti, To any](it Iter, fn func(Ti, Y[To]) error) Iter {
	return MakeGen(Gen[To]{
		Run: func(ctx context.Context, yield Y[To]) error {
			return ConsumeContext(ctx, it, func(v Ti) error {
				return fn(v, yield)
			})
		},
		Close: it.Close,
	})
}

// Peek calls the func fn each time the Next from the iterator is called
func Peek[T any](it Iter, fn func(v T)) Iter {
	return MakeIter(Custom[T]{
		Next: func(ctx context.Context) (T, error) {
			vv, err := it.Next(ctx)
			if err != nil {
				var z T
				return z, err
			}
			v, ok := vv.(T)
			if !ok {
				return v, fmt.Errorf("iter.Peek: type mismatch: %T", vv)
			}
			fn(v)
			return v, err
		},
		Close: it.Close,
	})
}
