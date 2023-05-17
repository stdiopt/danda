package etlcsv

import (
	"context"
	"encoding/csv"

	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
)

type encodeOptions struct {
	Comma  rune
	Header bool
}

type EncodeOptFunc func(*encodeOptions)

func WithEncodeComma(c rune) EncodeOptFunc {
	return func(o *encodeOptions) {
		o.Comma = c
	}
}

func WithEncodeNoHeader(v bool) EncodeOptFunc {
	return func(o *encodeOptions) {
		o.Header = v
	}
}

func makeEncodeOptions(opts ...EncodeOptFunc) encodeOptions {
	o := encodeOptions{
		Comma:  ',',
		Header: true,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// Encode consumes a drow.Row iterator and produces []byte
func Encode(it etl.Iter) etl.Iter {
	o := makeEncodeOptions()
	return etl.MakeGen(etl.Gen[[]byte]{
		Run: func(_ context.Context, yield etl.Y[[]byte]) error {
			w := etlio.YieldWriter(yield)

			cw := csv.NewWriter(w)
			cw.Comma = o.Comma
			defer cw.Flush()

			hdrWritten := false
			return etl.Consume(it, func(r drow.Row) error {
				if !hdrWritten && o.Header {
					hdrWritten = true
					hdr := make([]string, len(r))
					for i, f := range r {
						hdr[i] = f.Name
					}
					if err := cw.Write(hdr); err != nil {
						return err
					}
				}
				vals := make([]string, len(r))
				for i, f := range r {
					vals[i] = f.String()
				}
				return cw.Write(vals)
			})
		},
		Close: it.Close,
	})
}
