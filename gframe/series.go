package gframe

import (
	"fmt"

	"github.com/stdiopt/danda/util/conv"
)

// SeriesProvider is an interface for the underlying data on the series.
type SeriesProvider interface {
	Len() int
	Data() any
	At(int) any

	Clone() SeriesProvider
	WithValues(off int, data ...any) SeriesProvider
	Remove(indexes ...int) SeriesProvider
	Slice(start, sz int) SeriesProvider
}

// S creates a typed series
func S[T any](name string, data ...T) Series {
	s := Series{
		name:     name,
		provider: &SeriesData[T]{},
	}
	return s.WithValues(0, conv.ToAnySlice(data)...)
}

// SF Create a series based on func data
func SF[T any](name string, sz int, fn func(int) T) Series {
	// data := make([]T, sz)
	data := make([]any, sz)
	for i := 0; i < sz; i++ {
		data[i] = fn(i)
	}
	return Series{
		name:     name,
		provider: &SeriesData[T]{},
	}.WithValues(0, conv.ToAnySlice(data)...)
}

// SP returns a series based on provider.
func SP(name string, provider SeriesProvider) Series {
	return Series{name, provider}
}

// Series is a series of data.
type Series struct {
	name     string
	provider SeriesProvider
}

func (s Series) GoString() string {
	return fmt.Sprintf("series(%s) %v", s.name, s.provider)
}

// Clone returns a based series.
func (s Series) Clone() Series {
	if s.provider == nil {
		return Series{s.name, nil}
	}
	return Series{s.name, s.provider.Clone()}
}

// Slice a series based on start and size.
func (s Series) Slice(start, sz int) Series {
	if s.provider == nil {
		// Figure out this, maybe fill it with nils?
		return Series{s.name, nil}
	}
	return Series{s.name, s.provider.Slice(start, sz)}
}

// Name returns the series name
func (s Series) Name() string {
	return s.name
}

// Len returns the number of elements in the series
func (s Series) Len() int {
	if s.provider == nil {
		return 0
	}
	return s.provider.Len()
}

// At returns the element at index i or nil if i is out of bounds
func (s Series) At(i int) any {
	if s.provider == nil {
		return nil
	}
	return s.provider.At(i)
}

// WithValues returns a new series with the new values at index i.
func (s Series) WithValues(i int, data ...any) Series {
	if len(data) == 0 {
		return s.Clone()
	}
	provider := s.provider
	if provider == nil {
		provider = seriesDataFrom(data[0])
	}
	return Series{name: s.name, provider: provider.WithValues(i, data...)}
}

// Append return a new series with new data.
func (s Series) Append(data ...any) Series {
	if len(data) == 0 {
		return s.Clone()
	}

	provider := s.provider
	if provider == nil {
		provider = seriesDataFrom(data[0])
	}
	provider = provider.WithValues(provider.Len(), data...)

	return Series{name: s.name, provider: provider}
}

// Remove removes one or more indexes from the series.
func (s Series) Remove(indexes ...int) Series {
	if s.provider == nil {
		return s
	}
	return Series{name: s.name, provider: s.provider.Remove(indexes...)}
}

// WithName returns a cloned series with the new name.
func (s Series) WithName(name string) Series {
	if s.provider == nil {
		return Series{name: name}
	}
	return Series{name: name, provider: s.provider.Clone()}
}

// Data returns the underlying data slice
func (s Series) Data() any {
	if s.provider == nil {
		return nil
	}
	return s.provider.Data()
}

// Common Typed helpers, if value can't be converted it returns the zero value

func (s Series) Int(i int) int         { return conv.Conv(0, s.At(i)) }
func (s Series) Int32(i int) int32     { return conv.Conv(int32(0), s.At(i)) }
func (s Series) Int64(i int) int64     { return conv.Conv(int64(0), s.At(i)) }
func (s Series) Float32(i int) float32 { return conv.Conv(float32(0), s.At(i)) }
func (s Series) Float64(i int) float64 { return conv.Conv(float64(0), s.At(i)) }
func (s Series) String(i int) string   { return conv.ToString(s.At(i)) }

// AsInt converts the series to a int slice, if the value can't be converted
// it will be set as the zero value.
func (s Series) AsInt() []int {
	ret := make([]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Int(i)
	}
	return ret
}

// AsInt32 converts the series to a int32 slice, if the value can't be converted
// it will be set as the zero value.
func (s Series) AsInt32() []int32 {
	ret := make([]int32, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Int32(i)
	}
	return ret
}

// AsInt64 converts the series to a int64 slice, if the value can't be converted
// it will be set as the zero value.
func (s Series) AsInt64() []int64 {
	ret := make([]int64, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Int64(i)
	}
	return ret
}

// AsFloat32 converts the series to a float32 slice, if the value can't be
// converted it will be set as the zero value.
func (s Series) AsFloat32() []float32 {
	ret := make([]float32, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Float32(i)
	}
	return ret
}

// AsFloat64 converts the series to a float64 slice, if the value can't be
// converted it will be set as the zero value.
func (s Series) AsFloat64() []float64 {
	ret := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Float64(i)
	}
	return ret
}

// AsString converts the series to a string slice, if the value can't be converted it
// will be set as an empty string.
func (s Series) AsString() []string {
	ret := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.String(i)
	}
	return ret
}
