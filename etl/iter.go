// Package etl provides a simple iterator interface.
package etl

import (
	"context"
	"io"
)

// EOI is returned when iterator doesn't have more data.
var EOI = io.EOF

// Iter iterator interface.
type Iter interface {
	Next(context.Context) (any, error)
	Close() error
}

// New creates a new iterator based on func.
//func New[T any](next func() (T, error)) Iter {
//	return iter[T]{next: next}
//}

type Custom[T any] struct {
	Next  func(context.Context) (T, error)
	Close func() error
}

func MakeIter[T any](c Custom[T]) Iter {
	return &iter[T]{
		nextfn:  c.Next,
		closefn: c.Close,
	}
}

func NewIter[IT, DT any](
	start func() (IT, error),
	next func(context.Context, IT) (DT, error),
	end func(IT) error,
) Iter {
	var d *IT
	it := &iter[DT]{
		nextfn: func(ctx context.Context) (DT, error) {
			return next(ctx, *d)
		},
		closefn: func() error {
			return end(*d)
		},
	}

	if start != nil {
		tmpfn := it.nextfn
		it.nextfn = func(ctx context.Context) (DT, error) {
			var z DT
			dd, err := start()
			if err != nil {
				return z, err
			}
			d = &dd
			v, err := next(ctx, *d)
			it.nextfn = tmpfn
			return v, err
		}
	}

	return it
}

type iter[T any] struct {
	nextfn  func(context.Context) (T, error)
	closefn func() error
}

func (it *iter[T]) Next(ctx context.Context) (any, error) {
	if it.nextfn == nil {
		return nil, EOI
	}
	return it.nextfn(ctx)
}

func (it *iter[T]) Close() error {
	if it.closefn == nil {
		return nil
	}
	return it.closefn()
}
