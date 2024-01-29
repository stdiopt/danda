package etl

import (
	"fmt"
	"io"
)

// Collect collects all iterator values into a slice.
func Collect[T any](it Iter) ([]T, error) {
	var xs []T
	err := Consume(it, func(v T) error {
		xs = append(xs, v)
		return nil
	})
	return xs, err
}

func Consume[T any](it Iter, fn func(T) error) error {
	for {
		vv, err := it.Next()
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

// ConsumeBatch consumes the given iterator in batches of n values.
func ConsumeBatch[T any](it Iter, n int, fn func([]T) error) error {
	for {
		batch, err := Take[T](it, n)
		if err != nil {
			if err == EOI {
				if len(batch) > 0 {
					return fn(batch)
				}
				return nil
			}
			return err
		}
		// Assume no more data if batch is empty.
		if len(batch) == 0 {
			return nil
		}
		if err := fn(batch); err != nil {
			return err
		}
	}
}

// Limit returns an iterator that returns at most n values from the given iterator
// Closing the returned iterator will close the given iterator.
func Limit(it Iter, n int) Iter {
	return MakeIter(Custom[any]{
		Next: func() (any, error) {
			if n <= 0 {
				return nil, io.EOF
			}
			n--
			return it.Next()
		},
		Close: it.Close,
	})
}

// Take consumes the given iterator and return the first n values as a slice.
func Take[T any](it Iter, n int) ([]T, error) {
	var res []T
	for i := 0; i < n; i++ {
		v, err := it.Next()
		if err != nil {
			return res, err
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
	var n int
	for {
		_, err := it.Next()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
		n++
	}
}
