package etlutil

import (
	"context"
	"errors"
	"fmt"

	"github.com/stdiopt/danda/etl"
)

type joinOnFunc[L, R any] func(L, R) bool

// JoinData is the data yielded by the join functions.
type JoinData[L, R any] struct {
	Left  *L
	Right *R
}

func (j JoinData[L, R]) String() string {
	return fmt.Sprintf("%v %v", j.Left, j.Right)
}

// InnerJoin loads it2 into memory and calls fn on each element of it1 and it2.
func InnerJoin[L, R any](it1, it2 Iter, fn joinOnFunc[L, R]) Iter {
	return etl.MakeGen(etl.Gen[JoinData[L, R]]{
		Run: func(ctx context.Context, yield etl.Y[JoinData[L, R]]) error {
			cached, err := etl.Collect[R](it2)
			if err != nil {
				return err
			}
			return etl.ConsumeContext(ctx, it1, func(v1 L) error {
				for _, v2 := range cached {
					v1, v2 := v1, v2 // shadow
					if !fn(v1, v2) {
						continue
					}
					if err := yield(JoinData[L, R]{&v1, &v2}); err != nil {
						return err
					}
				}
				return nil
			})
		},
		Close: func() error {
			var errs []error
			if err := it1.Close(); err != nil {
				errs = append(errs, err)
			}
			if err := it2.Close(); err != nil {
				errs = append(errs, err)
			}
			if len(errs) > 0 {
				return errors.Join(errs...)
			}
			return nil
		},
	})
}

// LeftJoin loads it2 into memory and calls fn on each element of it1 and it2.
// If the fn returns true it will produce a JoinData[L,R] with the left value
// and optionaly right value
func LeftJoin[L, R any](it1, it2 Iter, fn joinOnFunc[L, R]) Iter {
	return etl.MakeGen(etl.Gen[JoinData[L, R]]{
		Run: func(ctx context.Context, yield etl.Y[JoinData[L, R]]) error {
			rightData, err := etl.Collect[R](it2)
			if err != nil {
				return err
			}
			return etl.ConsumeContext(ctx, it1, func(v1 L) error {
				found := false
				for _, v2 := range rightData {
					v1, v2 := v1, v2 // shadow
					if !fn(v1, v2) {
						continue
					}
					found = true
					if err := yield(JoinData[L, R]{&v1, &v2}); err != nil {
						return err
					}
				}
				if !found {
					if err := yield(JoinData[L, R]{&v1, nil}); err != nil {
						return err
					}
				}
				return nil
			})
		},
		Close: func() error {
			var errs []error
			if err := it1.Close(); err != nil {
				errs = append(errs, err)
			}
			if err := it2.Close(); err != nil {
				errs = append(errs, err)
			}
			if len(errs) > 0 {
				return errors.Join(errs...)
			}
			return nil
		},
	})
}

// RightJoin loads it1 into memory and calls fn on each element of it1 and it2,
// if the fn returns true it will produce a JoinData[L,R] with the right value
// and the left value.
func RightJoin[L, R any](it1, it2 Iter, fn joinOnFunc[L, R]) Iter {
	return etl.MakeGen(etl.Gen[JoinData[L, R]]{
		Run: func(ctx context.Context, yield etl.Y[JoinData[L, R]]) error {
			leftData, err := etl.Collect[L](it1)
			if err != nil {
				return err
			}
			return etl.ConsumeContext(ctx, it2, func(v2 R) error {
				found := false
				for _, v1 := range leftData {
					v1, v2 := v1, v2 // shadow
					if !fn(v1, v2) {
						continue
					}
					found = true
					if err := yield(JoinData[L, R]{&v1, &v2}); err != nil {
						return err
					}
				}
				if !found {
					return yield(JoinData[L, R]{nil, &v2})
				}
				return nil
			})
		},
		Close: func() error {
			var errs []error
			if err := it1.Close(); err != nil {
				errs = append(errs, err)
			}
			if err := it2.Close(); err != nil {
				errs = append(errs, err)
			}
			if len(errs) > 0 {
				return errors.Join(errs...)
			}
			return nil
		},
	})
}

// OuterJoin loads it2 into memory and check each element of it1 and it2
// produces JoinData[L,R] with the left and right value.
func OuterJoin[L, R any](it1, it2 Iter, fn joinOnFunc[L, R]) Iter {
	return etl.MakeGen(etl.Gen[JoinData[L, R]]{
		Run: func(ctx context.Context, yield etl.Y[JoinData[L, R]]) error {
			rightData, err := etl.Collect[R](it2)
			if err != nil {
				return err
			}
			added := map[int]struct{}{}
			err = etl.ConsumeContext(ctx, it1, func(v1 L) error {
				found := false
				for i, v2 := range rightData {
					if !fn(v1, v2) {
						continue
					}
					found = true
					added[i] = struct{}{}
					if err := yield(JoinData[L, R]{&v1, &v2}); err != nil {
						return err
					}
				}
				if !found {
					return yield(JoinData[L, R]{&v1, nil})
				}
				return nil
			})
			if err != nil {
				return err
			}
			for i, v2 := range rightData {
				_, ok := added[i]
				if ok {
					continue
				}
				if err := yield(JoinData[L, R]{nil, &v2}); err != nil {
					return err
				}
			}
			return nil
		},
		Close: func() error {
			var errs []error
			if err := it1.Close(); err != nil {
				errs = append(errs, err)
			}
			if err := it2.Close(); err != nil {
				errs = append(errs, err)
			}
			if len(errs) > 0 {
				return errors.Join(errs...)
			}
			return nil
		},
	})
}
