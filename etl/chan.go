package etl

import (
	"context"
	"errors"
	"io"
	"sync"
)

var ErrCancelled = errors.New("iterator cancelled")

// Y yield parameter.
type Y[T any] func(T) error

type msg[T any] struct {
	value T
	err   error
}

type Gen[T any] struct {
	Run   func(context.Context, Y[T]) error
	Close func() error
}

func MakeGen[T any](g Gen[T]) Iter {
	ch := make(chan msg[T])
	ictx, cancel := context.WithCancelCause(context.Background())

	yield := func(value T) error {
		select {
		case <-ictx.Done():
			return ictx.Err()
		case ch <- msg[T]{value: value}:
			return nil
		}
	}

	runner := func() {
		go func() {
			defer close(ch)
			if err := g.Run(ictx, yield); err != nil {
				select {
				case <-ictx.Done():
				case ch <- msg[T]{err: err}:
				}
			}
		}()
	}
	once := sync.Once{}

	return MakeIter(Custom[T]{
		Next: func(ctx context.Context) (T, error) {
			once.Do(runner)
			var z T
			select {
			case <-ctx.Done():
				cancel(ctx.Err())
				// do cancel? what if it is closed already?
				return z, ctx.Err()
			case <-ictx.Done():
				return z, ictx.Err()
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
		Next: func(ctx context.Context) (T, error) {
			var z T
			select {
			case <-ctx.Done():
				return z, ctx.Err()
			default:
				v, ok := <-ch
				if !ok {
					return z, io.EOF
				}
				return v, nil
			}
		},
	})
}
