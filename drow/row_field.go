package drow

import (
	"github.com/stdiopt/danda/util/conv"
)

// Field is a single field in a row
type Field struct {
	Name  string
	Value any
}

// F Creates a new typed field.
// TODO: {lpf} to review since we don't need generics here
func F[T any](name string, v T) Field {
	return Field{
		Name:  name,
		Value: v,
	}
}

// Common conversions

// String returns the string representation of the field
func (f Field) String() string { return conv.ToString(f.Value) }

// Int returns the int representation of the field or zero if it can't be converted
func (f Field) Int() int { return conv.Conv(0, f.Value) }

// Uint returns the uint representation of the field or zero if it can't be converted
func (f Field) Uint() uint { return conv.Conv(uint(0), f.Value) }

// Int16 returns the int16 representation of the field or zero if it can't be converted
func (f Field) Int16() int16 { return conv.Conv(int16(0), f.Value) }

// Uint16 returns the int16 representation of the field or zero if it can't be converted
func (f Field) Uint16() uint16 { return conv.Conv(uint16(0), f.Value) }

// Int32 returns the int32 representation of the field or zero if it can't be converted
func (f Field) Int32() int32 { return conv.Conv(int32(0), f.Value) }

// Uint32 returns the uint32 representation of the field or zero if it can't be converted
func (f Field) Uint32() uint32 { return conv.Conv(uint32(0), f.Value) }

// Int64 returns the int64 representation of the field or zero if it can't be converted
func (f Field) Int64() int64 { return conv.Conv(int64(0), f.Value) }

// Uint64 returns the uint64 representation of the field or zero if it can't be converted
func (f Field) Uint64() uint64 { return conv.Conv(uint64(0), f.Value) }

// Float32 returns the float32 representation of the field or zero if it can't be converted
func (f Field) Float32() float32 { return conv.Conv(float32(0), f.Value) }

// Float64 returns the float64 representation of the field or zero if it can't be converted
func (f Field) Float64() float64 { return conv.Conv(float64(0), f.Value) }
