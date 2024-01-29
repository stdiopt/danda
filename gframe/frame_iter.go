package gframe

import (
	"github.com/stdiopt/danda/etl"
)

// IterFrame creates a frame iterator that will return Row type per iteration
type IterFrame struct {
	df   *Frame
	curi int
}

// Next loads and returns true if there is a next value, false otherwise.
func (it *IterFrame) Next() (any, error) {
	if it.df.err != nil {
		return nil, it.df.err
	}
	if it.curi >= it.df.Len() {
		return nil, etl.EOI
	}
	row := it.df.Row(it.curi)
	it.curi++
	return row, nil
}

func (it *IterFrame) Close() error {
	return nil
}
