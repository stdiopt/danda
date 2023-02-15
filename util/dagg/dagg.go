// Package dagg provides a simple interface for aggregating data.
package dagg

import (
	"fmt"

	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/util/set"
)

type (
	// Row is a drow.Row
	Row = drow.Row
	// Field is a drow.Field
	Field = drow.Field
)

// GroupFn type of function that will produce they key for the data to be
// grouped.
type GroupFn[T any] func(T) (any, error)

// OptFn type of the option function for aggBuilder
type OptFn[T any] func(*Agg[T])

// optField field to be reduced and aggregated..
type optField[T any] struct {
	name       string
	reduceFunc func(any, T) any
	finalFunc  func(any) any
}

// Agg is an aggregation struct with methods to perform aggregations.
type Agg[T any] struct {
	// opts aggOptions
	groupMap set.Set[any]

	grpFn GroupFn[T]
	aggs  []optField[T]

	rows []Row

	curi int
}

// GroupBy sets the group function for the aggregation.
func (o *Agg[T]) GroupBy(fn GroupFn[T]) {
	o.grpFn = fn
}

// Reduce adds a reduce function for the aggregation.
func (o *Agg[T]) Reduce(name string, reduceFunc func(any, T) any, finalFunc func(any) any) {
	o.aggs = append(o.aggs, optField[T]{name, reduceFunc, finalFunc})
}

// Add adds a value to be processed and aggregated.
func (o *Agg[T]) Add(value T) error {
	if o.grpFn == nil {
		return fmt.Errorf("missing group func")
	}
	gv, err := o.grpFn(value)
	if err != nil {
		return err
	}

	ri, ok := o.groupMap.IndexOrAdd(gv)
	if !ok {
		row := make(Row, len(o.aggs))
		o.rows = append(o.rows, row)
	}

	for i, a := range o.aggs {
		v := o.rows[ri][i].Value
		v = a.reduceFunc(v, value)
		o.rows[ri][i] = Field{Name: a.name, Value: v}
	}
	return nil
}

// Each passes the produced aggregation row by calling fn
func (o *Agg[T]) Each(fn func(Row) error) error {
	for i, r := range o.rows {
		sr, ok := o.groupMap.Data[i].(Row)
		if !ok {
			sr = Row{Field{Name: "group_by", Value: o.groupMap.Data[i]}}
		}
		rc := make(Row, len(sr)+len(r))
		copy(rc, sr)

		rd := rc[len(sr):]
		// Apply final transformation
		for fi, a := range o.aggs {
			if a.finalFunc == nil {
				rd[fi] = r[fi]
				continue
			}
			rd[fi] = Field{Name: r[fi].Name, Value: a.finalFunc(r[fi].Value)}
		}
		if err := fn(rc); err != nil {
			return err
		}
	}
	return nil
}

// Next fetches a row and increments the index.
func (o *Agg[T]) Next() (Row, error) {
	if o.curi >= len(o.rows) {
		return nil, etl.EOI
	}

	r := o.rows[o.curi]

	sr, ok := o.groupMap.Data[o.curi].(Row)
	if !ok {
		sr = Row{Field{Name: "group", Value: o.groupMap.Data[o.curi]}}
	}
	// Merge into a new row
	// we copy the Group in the first fields
	// and process the following fields
	rc := make(Row, len(sr)+len(r))
	copy(rc, sr)

	rd := rc[len(sr):]
	// Apply final transformation or copy
	for i, a := range o.aggs {
		rd[i] = r[i]
		if a.finalFunc == nil {
			continue
		}
		rd[i].Value = a.finalFunc(rd[i].Value)
	}
	o.curi++
	return rc, nil
}
