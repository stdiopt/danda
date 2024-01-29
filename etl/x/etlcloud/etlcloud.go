// Package etlcloud provides etl iters based on gocloud.dev
package etlcloud

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
	"gocloud.dev/blob"
)

type listOptions struct {
	Delimiter string
}

type ListOptFunc func(*listOptions)

func WithDelimiter(d string) ListOptFunc {
	return func(o *listOptions) {
		o.Delimiter = d
	}
}

func makeListOptions(opts ...ListOptFunc) listOptions {
	o := listOptions{}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

func BlobListObjects(bucketURL string, opts ...ListOptFunc) etl.Iter {
	o := makeListOptions(opts...)

	ctx, cancel := context.WithCancel(context.Background())
	var prefix string
	b, err := func() (*blob.Bucket, error) {
		u, err := url.Parse(bucketURL)
		if err != nil {
			return nil, err
		}
		// in the form of '{scheme}://{host}/{prefix}'
		burl := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
		if u.Path != "" {
			prefix = strings.Trim(u.Path, "/") + "/"
		}
		if u.RawQuery != "" {
			burl += "?" + u.RawQuery
		}
		return blob.OpenBucket(ctx, burl)
	}()
	if err != nil {
		cancel()
		return etl.ErrIter(err)
	}

	bIt := b.List(&blob.ListOptions{Prefix: prefix, Delimiter: o.Delimiter})
	return etl.MakeIter(etl.Custom[*blob.ListObject]{
		Next: func() (*blob.ListObject, error) {
			return bIt.Next(context.Background())
		},
		Close: func() error {
			cancel()
			return b.Close()
		},
	})
}

func BlobGetObject(objURL string) etl.Iter {
	ctx, cancel := context.WithCancel(context.Background())

	rd, err := func() (io.ReadCloser, error) {
		u, err := url.Parse(objURL)
		if err != nil {
			return nil, err
		}
		// in the form of '{scheme}://{host}/{prefix}'
		burl := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
		if u.RawQuery != "" {
			burl += "?" + u.RawQuery
		}
		b, err := blob.OpenBucket(ctx, burl)
		if err != nil {
			return nil, err
		}

		key := strings.Trim(u.Path, "/")
		return b.NewReader(ctx, key, nil)
	}()
	if err != nil {
		cancel()
		return etl.ErrIter(err)
	}

	eof := false
	return etl.MakeIter(etl.Custom[[]byte]{
		Next: func() ([]byte, error) {
			if eof {
				return nil, etl.EOI
			}
			select {
			case <-ctx.Done():
				cancel()
				return nil, ctx.Err()
			default:
			}
			// Make buf size configurable
			buf := make([]byte, 1024)
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

func BlobPutObject(it etl.Iter, objURL string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u, err := url.Parse(objURL)
	if err != nil {
		return err
	}
	// in the form of '{scheme}://{host}/{prefix}'
	burl := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	if u.RawQuery != "" {
		burl += "?" + u.RawQuery
	}
	b, err := blob.OpenBucket(ctx, burl)
	if err != nil {
		return err
	}
	defer b.Close()

	key := strings.Trim(u.Path, "/")
	wr, err := b.NewWriter(ctx, key, nil)
	if err != nil {
		return err
	}
	defer wr.Close()
	_, err = io.Copy(wr, etlio.AsReader(it))
	return err
}
