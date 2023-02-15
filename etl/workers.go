package etl

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type W[To any] struct {
	Iter
	ID    int
	yield Y[To]
}

func (w W[To]) Yield(v To) error {
	return w.yield(v)
}

// Workers iterates over a slice of values and calls a worker func for each value.
func Workers[To any](it Iter, workers int, fn func(context.Context, W[To]) error) Iter {
	return MakeGen(Gen[To]{
		Run: func(yield Y[To]) error {
			itval := make(chan any)

			eg, ctx := errgroup.WithContext(context.Background())
			for i := 0; i < workers; i++ {
				w := W[To]{
					Iter:  Chan(itval),
					ID:    i,
					yield: yield,
				}
				eg.Go(func() error {
					return fn(ctx, w)
				})
			}
			eg.Go(func() error {
				defer close(itval)
				return Consume(it, func(v any) error {
					select {
					case itval <- v:
					case <-ctx.Done():
						return ctx.Err()
					}

					return nil
				})
			})
			return eg.Wait()
		},
		Close: it.Close,
	})
}

// WorkersValue is a convinitent func that calls fn for every consumed value, it will yield any value
// by calling the yield func.
func WorkersValue[Ti, To any](it Iter, workers int, fn func(context.Context, Ti, Y[To]) error) Iter {
	return Workers(it, workers, func(ctx context.Context, w W[To]) error {
		return ConsumeContext(ctx, w, func(v Ti) error {
			return fn(ctx, v, w.Yield)
		})
	})
}

// WorkersConsume creates a pool of workers that call fn for every iteration value.
// this will close the consumed Iter upon finish.
func WorkersConsume[Ti any](it Iter, workers int, fn func(context.Context, Ti) error) error {
	return WorkersConsumeContext(context.Background(), it, workers, fn)
}

func WorkersConsumeContext[Ti any](ctx context.Context, it Iter, workers int, fn func(context.Context, Ti) error) error {
	defer it.Close()

	eg, ctx := errgroup.WithContext(ctx)
	itval := make(chan Ti)
	for i := 0; i < workers; i++ {
		eg.Go(func() error {
			for {
				select {
				case v, ok := <-itval:
					if !ok {
						return nil
					}
					if err := fn(ctx, v); err != nil {
						return err
					}
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}
	eg.Go(func() error {
		defer close(itval)
		return ConsumeContext(ctx, it, func(v Ti) error {
			select {
			case itval <- v:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	})
	return eg.Wait()
}
