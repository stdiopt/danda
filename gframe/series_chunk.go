package gframe

import (
	"fmt"
	"sort"
)

// Generic type is only here to ensure same type over.
type seriesChunk[T any] struct {
	offset int
	// data   []T
	data []any
}

// SeriesData handles underlying data of a series.
type SeriesData[T any] struct {
	chunks []seriesChunk[T]
}

// dynamic series creates a generic series from an interface
// if the type is not matched it will create a `any` series
func seriesDataFrom(v any) SeriesProvider {
	switch v.(type) {
	case int:
		return &SeriesData[int]{}
	case int32:
		return &SeriesData[int32]{}
	case int64:
		return &SeriesData[int64]{}
	case string:
		return &SeriesData[string]{}
	case float32:
		return &SeriesData[float32]{}
	case float64:
		return &SeriesData[float64]{}
	case Row:
		return &SeriesData[Row]{}
	case []Row:
		return &SeriesData[[]Row]{}
	default:
		// log.Printf("Warning uncategorized data: %T", v)
		return &SeriesData[any]{}
	}
}

func (s SeriesData[T]) String() string {
	return fmt.Sprintf("len: %v, chunks: %d",
		s.Len(),
		len(s.chunks),
	)
}

// Len returns the series length
func (s SeriesData[T]) Len() int {
	m := 0
	for _, c := range s.chunks {
		l := c.offset + len(c.data)
		if l > m {
			m = l
		}
	}
	return m
}

// Data returns the series data which can be casted to .([]T)
func (s SeriesData[T]) Data() any {
	return s.data()
}

// Get returns the value at index i.
func (s SeriesData[T]) At(i int) any {
	var z T
	for ci := len(s.chunks) - 1; ci >= 0; ci-- {
		c := s.chunks[ci]
		if i < c.offset {
			continue
		}

		if i >= c.offset+len(c.data) {
			continue
		}
		return c.data[i-c.offset]
	}
	return z
}

// WithValues returns a copy of the series with the values set on the give index.
func (s SeriesData[T]) WithValues(off int, vs ...any) SeriesProvider {
	ns := s.clone()

	data := make([]any, len(vs))
	for i, v := range vs {
		if _, ok := v.(T); !ok {
			continue
		}
		data[i] = v
	}
	chunk := seriesChunk[T]{
		offset: off,
		// data:   dconv.AnySliceTo(*new(T), vs...),
		data: data,
	}

	ns.chunks = append(ns.chunks, chunk)
	return ns
}

// Clone returns a copy of the series data
func (s SeriesData[T]) Clone() SeriesProvider {
	return s.clone()
}

// Slice flattens the series and take a subset
func (s SeriesData[T]) Slice(start, sz int) SeriesProvider {
	data := s.data()
	if start > len(data) {
		return &SeriesData[T]{}
	}

	end := min(start+sz, len(data))

	// nd := make([]T, end-start)
	nd := make([]any, end-start)
	copy(nd, data[start:end])

	return SeriesData[T]{chunks: []seriesChunk[T]{{offset: 0, data: nd}}}
}

// Remove one or more indexes from the series
func (s SeriesData[T]) Remove(indexes ...int) SeriesProvider {
	// Avoid sorting on variadic parameter
	indexes = append([]int{}, indexes...)
	sort.Ints(indexes)

	data := s.data()
	for i := len(indexes) - 1; i >= 0; i-- {
		di := indexes[i]
		if di >= len(data) {
			continue
		}
		data = append(data[:di], data[di+1:]...)
	}
	return &SeriesData[T]{
		chunks: []seriesChunk[T]{
			{data: data},
		},
	}
}

func (s SeriesData[T]) clone() *SeriesData[T] {
	return &SeriesData[T]{
		chunks: []seriesChunk[T]{
			{data: s.data()},
		},
	}
}

// should be []T in prev chunk
//
//	func (s SeriesData[T]) data() []T {
//		ret := make([]T, s.Len())
func (s SeriesData[T]) data() []any {
	ret := make([]any, s.Len())
	for _, c := range s.chunks {
		copy(ret[c.offset:], c.data)
	}
	return ret
}
