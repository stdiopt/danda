// Package etlzip iterates over zip files data.
package etlzip

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/krolaw/zipstream"
	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
)

type Iter = etl.Iter

// EachIter iterator for each file stream.
type Entry struct {
	Iter
	Name           string
	CompressedSize uint32
	Size           uint64
}

type eachFn[T any] func(Entry, etl.Y[T]) error

// EachFile consumes a []byte iterator and sends iterators for unziped file
func EachFile[T any](it Iter, pattern string, fn eachFn[T]) Iter {
	return etl.MakeGen(etl.Gen[T]{
		Run: func(_ context.Context, yield etl.Y[T]) error {
			r := etlio.AsReader(it)

			zs := zipstream.NewReader(r)
			for {
				hdr, err := zs.Next()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return fmt.Errorf("iterzip.Stream: failed to consume iter: %w", err)
				}

				ok, err := filepath.Match(pattern, filepath.Base(hdr.Name))
				if err != nil {
					return fmt.Errorf("iterzip.Stream: failed to match pattern '%s': %w", pattern, err)
				}
				if !ok {
					continue
				}

				// Create an iterator for the file
				err = func() error {
					it := etlio.FromReader(zs, nil)
					eit := Entry{
						Iter:           it,
						Name:           hdr.Name,
						CompressedSize: hdr.CompressedSize,
						Size:           hdr.UncompressedSize64,
					}
					return fn(eit, yield)
				}()
				if err != nil {
					return err
				}
			}
		},
		Close: it.Close,
	})
}
