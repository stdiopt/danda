package etl

import (
	"context"
	"fmt"
	"io"
)

// Collect collects all iterator values into a slice.
func CollectContext[T any](ctx context.Context, it Iter) ([]T, error) {
	var xs []T
	err := ConsumeContext(ctx, it, func(v T) error {
		xs = append(xs, v)
		return nil
	})
	return xs, err
}

func Collect[T any](it Iter) ([]T, error) {
	return CollectContext[T](context.Background(), it)
}

func ConsumeContext[T any](ctx context.Context, it Iter, fn func(T) error) error {
	for {
		vv, err := it.Next(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		v, ok := vv.(T)
		if !ok {
			return fmt.Errorf("Consume: type mismatch: %T", vv)
		}

		// Do nothing if nil but still consume.
		if fn == nil {
			continue
		}
		if err := fn(v); err != nil {
			return err
		}
	}
}

// Consume iterates over the given iterator and calls fn for each value.
func Consume[T any](it Iter, fn func(T) error) error {
	return ConsumeContext(context.Background(), it, fn)
}

// Limit returns an iterator that returns at most n values from the given iterator
// Closing the returned iterator will close the given iterator.
func Limit(it Iter, n int) Iter {
	return MakeIter(Custom[any]{
		Next: func(ctx context.Context) (any, error) {
			if n <= 0 {
				return nil, io.EOF
			}
			n--
			return it.Next(ctx)
		},
		Close: it.Close,
	})
}

// Take will consume the given iterator and return the first n values as a slice.
func Take[T any](it Iter, n int) ([]T, error) {
	ctx := context.Background()
	var res []T
	for i := 0; i < n; i++ {
		v, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		res = append(res, v.(T))
	}
	return res, nil
}

// Print prints the values of the given iterator to stdout.
func Print(it Iter) error {
	return Consume(it, func(v any) error {
		fmt.Println(v)
		return nil
	})
}

// Count consumes the iterator and return the number of iterations.
func Count(it Iter) (int, error) {
	ctx := context.Background()
	var n int
	for {
		_, err := it.Next(ctx)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
		n++
	}
}
