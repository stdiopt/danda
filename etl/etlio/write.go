package etlio

import (
	"fmt"
	"io"

	"github.com/stdiopt/danda/etl"
)

// WriteTo writes the data from the given []byte iterator to the given writer.
func WriteTo(it Iter, w io.Writer) error {
	defer it.Close() // Should not close?!
	for {
		v, err := it.Next()
		if err == etl.EOI {
			break
		}
		if err != nil {
			return err
		}

		data, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("etlio.WriteTo: expected []byte, got %T", v)
		}

		_, err = w.Write(data)
		if err != nil {
			return err
		}
	}
	return nil
}
