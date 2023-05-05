// Package etldrow contains functions to manipulate iter.Iter of drow.Row.
package etldrow

import (
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
)

// Iter is an etl.Iter.
type Iter = etl.Iter

// Row is a iter.Row
type Row = drow.Row

type IntOrString = drow.IntOrString

// Field returns a new iter that will return the value of the field 'name'.
func Field(it Iter, name string) Iter {
	return etl.Map(it, func(row Row) any {
		return row.At(name).Value
	})
}

// Select returns an iterator that yields rows with only the fields in 'names'.
func Select(it Iter, names ...IntOrString) Iter {
	return etl.Map(it, func(row Row) Row {
		return row.Select(names...)
	})
}

// Rename returns an iterator that yields row renamed.
func Rename(it Iter, o, n string) Iter {
	return etl.Map(it, func(row Row) Row {
		return row.Rename(o, n)
	})
}

// ToMap converts iterator rows to map.
func ToMap(it Iter) Iter {
	return etl.Map(it, func(row Row) map[string]any {
		return row.ToMap()
	})
}
