// Package etlcsv contains iterators that handle csv data.
package etlcsv

import (
	"context"
	"encoding/csv"
	"strings"

	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
)

type (
	// Row is a danda.Row
	Row = drow.Row
	// Field is a danda.Field
	Field = drow.Field
	// Iter is a iter.Iter
	Iter = etl.Iter
)

type decodeOptions struct {
	// Comma is the field delimiter.
	Comma rune
}

type DecodeOptFunc func(*decodeOptions)

func WithDecodeComma(c rune) DecodeOptFunc {
	return func(o *decodeOptions) {
		o.Comma = c
	}
}

func makeOptions(opts ...DecodeOptFunc) decodeOptions {
	o := decodeOptions{
		Comma: ',',
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// Decode returns an iterator that reads danda.Iter based on []byte and produces danda.Row
// Close will close the underlying iterator.
func Decode(it Iter, opts ...DecodeOptFunc) Iter {
	o := makeOptions(opts...)

	pr := etlio.AsReader(it)
	var cr *csv.Reader
	var cols []string
	return etl.MakeIter(etl.Custom[Row]{
		Next: func(context.Context) (Row, error) {
			if cr == nil {
				cr = csv.NewReader(pr)
				cr.Comma = o.Comma
				c, err := cr.Read()
				if err != nil {
					return nil, err
				}
				cols = c
			}

			for {
				dataRow, err := cr.Read()
				if err != nil {
					return nil, err
				}
				if len(dataRow) == 0 {
					continue
				}
				row := make(Row, len(cols))
				for i, r := range dataRow {
					row[i] = Field{Name: cols[i], Value: strings.TrimSpace(r)}
				}
				return row, nil
			}
		},
		Close: it.Close,
	})
}
