package etlgzip

import (
	"compress/gzip"
	"context"
	"io"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
)

type gunzipOptions struct {
	BufSize int
}

type gunzipOptFunc func(*gunzipOptions)

func WithBufSize(size int) gunzipOptFunc {
	return func(o *gunzipOptions) {
		o.BufSize = size
	}
}

func makeGunzipOptions(opts ...gunzipOptFunc) gunzipOptions {
	o := gunzipOptions{
		BufSize: 4096,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// Gunzip consumes gzipped compressed data and produces []byte of uncompressed
// data.
func Gunzip(it etl.Iter, opts ...gunzipOptFunc) etl.Iter {
	o := makeGunzipOptions(opts...)
	var rd io.Reader
	eof := false
	return etl.MakeIter(etl.Custom[[]byte]{
		Next: func(_ context.Context) ([]byte, error) {
			if eof {
				return nil, etl.EOI
			}
			if rd == nil {
				r := etlio.AsReader(it)
				gr, err := gzip.NewReader(r)
				if err != nil {
					return nil, err
				}
				rd = gr
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
		Close: it.Close,
	})
}

type gzipOptions struct {
	Level int
}

type gzipOptFunc func(*gzipOptions)

func WithLevel(level int) gzipOptFunc {
	return func(o *gzipOptions) {
		o.Level = level
	}
}

func makeGzipOptions(opts ...gzipOptFunc) gzipOptions {
	o := gzipOptions{
		Level: gzip.DefaultCompression,
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

func Gzip(it etl.Iter, opts ...gzipOptFunc) etl.Iter {
	o := makeGzipOptions(opts...)
	return etl.Yield(func(ctx context.Context, yield etl.Y[[]byte]) error {
		r := etlio.AsReader(it)
		w := etlio.YieldWriter(yield)
		gw, err := gzip.NewWriterLevel(w, o.Level)
		if err != nil {
			return err
		}
		defer gw.Close()

		_, err = io.Copy(gw, r)
		return err
	})
}
