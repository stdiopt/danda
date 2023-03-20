package etlio

import (
	"context"
	"errors"

	"github.com/stdiopt/danda/etl"
)

// ReadAll reads all data from a []byte iterator
func ReadAll(it Iter) ([]byte, error) {
	return ReadAllContext(context.Background(), it)
}

func ReadAllContext(ctx context.Context, it Iter) ([]byte, error) {
	ret := []byte{}
	for {
		b, err := it.Next(ctx)
		if err == etl.EOI {
			return ret, nil
		}
		if err != nil {
			return nil, err
		}
		v, ok := b.([]byte)
		if !ok {
			return nil, errors.New("not a []byte")
		}
		ret = append(ret, v...)
	}
}
