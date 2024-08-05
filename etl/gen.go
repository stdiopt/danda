package etl

import "context"

// ErrIter is an iterator that always returns an error.
func ErrIter(err error) Iter {
	return MakeIter(Custom[any]{
		Next: func(context.Context) (any, error) { return nil, err },
	})
}

// Values returns an iterator that iterates over the variadic arguments.
func Values[T any](vs ...T) Iter {
	return MakeIter(Custom[T]{
		Next: func(context.Context) (T, error) {
			var z T
			if len(vs) == 0 {
				return z, EOI
			}
			v := vs[0]
			vs = vs[1:]
			return v, nil
		},
	})
}

// Yield runs fn which yields values to the iterator.
func Yield[T any](fn func(ctx context.Context, yield Y[T]) error) Iter {
	done := make(chan struct{})
	return MakeGen(Gen[T]{
		Run: func(ctx context.Context, yield Y[T]) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			select {
			case <-done:
				cancel()
				return nil
			default:
			}
			return fn(ctx, yield)
		},
		Close: func() error {
			close(done)
			return nil
		},
	})
}

// Seq iterates over the sequence of integers from start to end.
func Seq(start, end, step int) Iter {
	if start > end && step > 0 {
		panic("invalid range")
	}
	if start < end && step < 0 {
		panic("invalid range")
	}

	return MakeIter(Custom[int]{
		Next: func(context.Context) (int, error) {
			if start == end {
				return 0, EOI
			}
			v := start
			start += step
			return v, nil
		},
	})
}
