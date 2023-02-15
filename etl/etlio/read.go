package etlio

import (
	"context"
	"errors"

	"github.com/stdiopt/danda/etl"
)

func ReadAll(ctx context.Context, it Iter) ([]byte, error) {
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
