package etlfs

import (
	"fmt"
	"os"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
)

// ReadFile reads a file and yields the content as multiple sets of []byte
// Closing the iterator will close the file.
func ReadFile(p string) Iter {
	f, err := os.Open(p)
	if err != nil {
		err = fmt.Errorf("iterio.ReadFile: error reading file: %w", err)
		return etl.ErrIter(err)
	}
	return etlio.FromReadCloser(f)
}

// WriteFile reads []byte from an iterator and writes it to a file
// the function closes the iterator.
func WriteFile(it Iter, p string) error {
	defer it.Close()
	f, err := os.Create(p)
	if err != nil {
		err = fmt.Errorf("iterio.WriteFile: error creating file: %w", err)
		return err
	}
	defer f.Close()
	return etl.Consume(it, func(data []byte) error {
		// TODO: {lpf} check if whole buffer was written.
		_, err := f.Write(data)
		return err
	})
}
