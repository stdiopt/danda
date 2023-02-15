// Package etlio contains logic do handle io stuff.
package etlio

import (
	"context"
	"fmt"
	"io"

	"github.com/stdiopt/danda/etl"
)

// Iter is an etl.Iter.
type Iter = etl.Iter

type readerOptions struct {
	BufSize int
}

type ReaderOptFunc func(*readerOptions)

func WithBufSize(size int) ReaderOptFunc {
	return func(o *readerOptions) {
		o.BufSize = size
	}
}

func makeReaderOptions(opts ...ReaderOptFunc) readerOptions {
	o := readerOptions{
		BufSize: 4096,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// FromReadCloser returns an iterator that reads from the io.ReadCloser
// Closing the iter will close the io.ReadCloser
func FromReadCloser(rd io.ReadCloser, opts ...ReaderOptFunc) Iter {
	o := makeReaderOptions(opts...)
	eof := false
	return etl.MakeIter(etl.Custom[[]byte]{
		Next: func(context.Context) ([]byte, error) {
			if eof {
				return nil, etl.EOI
			}
			buf := make([]byte, o.BufSize)
			n, err := rd.Read(buf)
			switch {
			case err == io.EOF:
				eof = true
				if n == 0 {
					return nil, etl.EOI
				}
			case err != nil:
				return nil, err
			}
			return buf[:n], nil
		},
		Close: rd.Close,
	})
}

// FromReader returns an iterator that reads from the io.Reader
func FromReader(rd io.Reader, opts ...ReaderOptFunc) Iter {
	return FromReadCloser(io.NopCloser(rd), opts...)
}

// AsReader returns an io.Reader that reads from the iterator
func AsReader(it Iter) io.ReadCloser {
	return &iterReadCloser{it: it}
}

type YieldWriter etl.Y[[]byte]

func (yield YieldWriter) Write(data []byte) (int, error) {
	cp := append([]byte{}, data...)

	err := yield(cp)
	if err != nil {
		return 0, err
	}

	return len(data), err
}

// iterRead implements io.Reader by iterating through an iter.
type iterReadCloser struct {
	it  Iter
	buf []byte
}

// Read implements io.Reader
func (r *iterReadCloser) Read(data []byte) (int, error) {
	if len(r.buf) == 0 {
		v, err := r.it.Next(context.TODO())
		if err != nil {
			return 0, err
		}
		vb, ok := v.([]byte)
		if !ok {
			return 0, fmt.Errorf("expected []byte, got %T", v)
		}
		r.buf = vb
	}
	n := copy(data, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *iterReadCloser) Close() error {
	return r.it.Close()
}
