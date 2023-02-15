// Package etlhttp contains iterators that handle http.
package etlhttp

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/stdiopt/danda/etl"
)

type (
	// Iter is an iter.Iter
	Iter = etl.Iter
)

type getOptions struct {
	BufSize int
	Header  http.Header
}

type GetOptFunc func(*getOptions)

func WithGetBufSize(size int) GetOptFunc {
	return func(o *getOptions) {
		o.BufSize = size
	}
}

func WithGetHeader(header http.Header) GetOptFunc {
	return func(o *getOptions) {
		if o.Header == nil {
			o.Header = make(http.Header)
		}
		for k, v := range header {
			o.Header[k] = v
		}
	}
}

func makeGetOptions(opts ...GetOptFunc) getOptions {
	o := getOptions{
		BufSize: 8192,
	}

	for _, fn := range opts {
		fn(&o)
	}
	return o
}

func (g *getOptions) applyToReq(req *http.Request) {
	if g.Header != nil {
		req.Header = g.Header
	}
}

func Get(url string, opts ...GetOptFunc) Iter {
	o := makeGetOptions(opts...)

	// Can't defer cancel since it's used in the iter
	ctx, cancel := context.WithCancel(context.Background())
	res, err := func() (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("etlhttp.Get: error creating request: %w", err)
		}
		o.applyToReq(req)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("etlhttp.Get: error calling request: %w", err)
		}
		if res.StatusCode < 200 || res.StatusCode >= 400 {
			return nil, fmt.Errorf("http status code: %d - %v", res.StatusCode, res.Status)
		}

		return res, nil
	}()
	if err != nil {
		cancel()
		return etl.ErrIter(err)
	}

	eof := false
	return etl.MakeIter(etl.Custom[[]byte]{
		Next: func(ctx context.Context) ([]byte, error) {
			select {
			case <-ctx.Done():
				cancel()
				return nil, ctx.Err()
			default:
			}
			if eof {
				return nil, etl.EOI
			}
			buf := make([]byte, o.BufSize)
			n, err := res.Body.Read(buf)
			if err != nil && err != io.EOF {
				res.Body.Close() // nolint: errcheck
				return nil, err
			}
			if err == io.EOF {
				eof = true
				if n == 0 {
					return nil, etl.EOI
				}

			}
			return buf[:n], nil
		},
		Close: func() error {
			if res.Body != nil {
				return res.Body.Close()
			}
			return nil
		},
	})
}
