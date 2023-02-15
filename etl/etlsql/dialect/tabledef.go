package dialect

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/stdiopt/danda/drow"
)

type (
	Row = drow.Row
)

// Col represets a column in a table
type Col struct {
	Name string
	Type reflect.Type
	// Overrides for sql types
	SQLType string
	SQLDef  string
}

// Eq compares two columns by name and type.
func (c Col) Eq(c2 Col) bool {
	return c.Name == c2.Name && c.Type == c2.Type
}

// Zero returns the zero value for the column type.
func (c Col) Zero() any {
	if c.Type == nil {
		return nil
	}
	return reflect.New(c.Type).Elem().Interface()
}

// Table represents an sql table definition.
type Table struct {
	Columns []Col
}

// FromRows scans a slice of rows and returns a TableDef with the columns.
func FromRows(rows []Row) Table {
	def := Table{}
	for _, r := range rows {
		for _, f := range r {
			def.AddCol(Col{
				Name: strings.ToLower(f.Name),
				Type: reflect.TypeOf(f.Value),
			})
		}
	}
	return def
}

// Len returns the number of columns in the table.
func (d Table) Len() int {
	return len(d.Columns)
}

// Get returns the column with the given name or an empty col if non existent.
func (d Table) Get(colName string) Col {
	for _, c := range d.Columns {
		if c.Name == colName {
			return c
		}
	}
	return Col{}
}

// AddCol adds a column to the table.
func (d *Table) AddCol(col Col) {
	for _, c := range d.Columns {
		if c.Name == col.Name {
			if c.Type == nil && col.Type != nil {
				c.Type = col.Type
			}
			return
		}
	}
	d.Columns = append(d.Columns, col)
}

// MissingOn returns a TableDef with missing columns from d2
func (d Table) MissingOn(d2 Table) Table {
	var ret Table
	for _, c := range d.Columns {
		if !d2.hasCol(c.Name) {
			ret.Columns = append(ret.Columns, c)
		}
	}
	return ret
}

// StrJoin returns a string with all column names joined by sep.
func (d Table) StrJoin(sep string) string {
	buf := bytes.Buffer{}
	for i, c := range d.Columns {
		if i != 0 {
			buf.WriteString(sep)
		}
		buf.WriteString(c.Name)
	}
	return buf.String()
}

// NormalizeRows returns a slice of rows based on definition d.
func (d Table) NormalizeRows(rows []Row) []Row {
	ret := make([]Row, 0, len(rows))
	for _, r := range rows {
		row := Row{}
		for _, c := range d.Columns {
			f := r.At(EqualFold(c.Name))
			row = row.WithField(c.Name, f.Value)
		}
		ret = append(ret, row)
	}
	return ret
}

// RowValues returns a slice of values from the given rows.
// |row1|row2|row3| => |row1[0]|row1[1]|row2[0]|row2[1]|row3[0]|row3[1]|
func (d Table) RowValues(rows []Row) []any {
	params := []any{}
	for _, r := range rows {
		for _, c := range d.Columns {
			f := r.At(EqualFold(c.Name))
			v := f.Value
			if v == nil {
				v = c.Zero()
			}
			params = append(params, v)
		}
	}
	return params
}

func (d Table) String() string {
	buf := &bytes.Buffer{}
	for _, c := range d.Columns {
		fmt.Fprintf(buf, "  %s %s\n", c.Name, c.Type)
	}
	return buf.String()
}

func (d Table) hasCol(k string) bool {
	for _, c := range d.Columns {
		if c.Name == k {
			return true
		}
	}
	return false
}

// equalFold returns a func used in drow.Row.At to fetch a insesitive case field
func EqualFold(s string) func(row Row) *drow.Field {
	return func(row Row) *drow.Field {
		for _, f := range row {
			if strings.EqualFold(f.Name, s) {
				return &f
			}
		}
		return nil
	}
}
