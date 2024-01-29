package etl

import (
	"context"
	"errors"
	"io"
)

var ErrCancelled = errors.New("iterator cancelled")

// Y yield parameter.
type Y[T any] func(T) error

type msg[T any] struct {
	value T
	err   error
}

type Gen[T any] struct {
	Run   func(Y[T]) error
	Close func() error
}

func MakeGen[T any](g Gen[T]) Iter {
	ch := make(chan msg[T])
	ctx, cancel := context.WithCancelCause(context.Background())

	yield := func(value T) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- msg[T]{value: value}:
			return nil
		}
	}

	go func() {
		defer close(ch)
		if err := g.Run(yield); err != nil {
			select {
			case <-ctx.Done():
			case ch <- msg[T]{err: err}:
			}
		}
	}()

	return MakeIter(Custom[T]{
		Next: func() (T, error) {
			var z T
			select {
			case <-ctx.Done():
				return z, ctx.Err()
			case msg, ok := <-ch:
				if !ok {
					return z, io.EOF
				}
				return msg.value, msg.err
			}
		},
		Close: func() error {
			var err error
			if g.Close != nil {
				err = g.Close()
			}
			cancel(err)
			return err
		},
	})
}

func Chan[T any](ch <-chan T) Iter {
	return MakeIter(Custom[T]{
		Next: func() (T, error) {
			var z T
			v, ok := <-ch
			if !ok {
				return z, io.EOF
			}
			return v, nil
		},
	})
}
