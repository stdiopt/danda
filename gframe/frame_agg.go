package gframe

import "github.com/stdiopt/danda/util/dagg"

type (
	aggGroupFn = dagg.GroupFn[Row]
	aggBuilder = dagg.Agg[Row]
	aggOptFn   = dagg.OptFn[Row]
)

// Group initiates a group operation on a frame.
func (f Frame) Group(grpfn aggGroupFn, opts ...aggOptFn) Frame {
	a := aggBuilder{}
	a.GroupBy(grpfn)
	for _, opt := range opts {
		opt(&a)
	}

	if err := f.Each(a.Add); err != nil {
		return ErrFrame(err)
	}

	fb := rowFrameBuilder{}
	if err := a.Each(fb.Add); err != nil {
		return ErrFrame(err)
	}

	return fb.Frame()
}

// GroupByFuncE is a convenience function for grouping a frame by a function.
func GroupByFuncE(fn func(v Row) (Row, error)) aggGroupFn {
	return func(v Row) (any, error) {
		return fn(v)
	}
}

// Reduce adds a reduce function to aggregator.
func Reduce[T any](name string, fn func(T, Row) T, ffn ...func(T) any) aggOptFn {
	var z T
	reducefn := func(a any, v Row) any {
		acc, ok := a.(T)
		if !ok {
			acc = z
		}
		return fn(acc, v)
	}
	var finalfn func(any) any
	if len(ffn) > 0 {
		finalfn = func(a any) any {
			acc, ok := a.(T)
			if !ok {
				acc = z
			}
			return ffn[0](acc)
		}
	}
	return func(a *aggBuilder) {
		a.Reduce(name, reducefn, finalfn)
	}
}
