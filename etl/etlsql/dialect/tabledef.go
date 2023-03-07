package dialect

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/apd"
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
)

type (
	Row = drow.Row
)

// Col represets a column in a table
type Col struct {
	Name     string
	Type     reflect.Type // RAW? ScanType
	Nullable bool
	Length   int64 // for varchar and maybe other types
	Scale    int   // Precision int ...
	// Overrides for sql types
	SQLType string // override
}

// Eq compares two columns by name and type.
func (c Col) Eq(c2 Col) bool {
	return c.Name == c2.Name &&
		c.Type == c2.Type &&
		c.Length == c2.Length &&
		c.Scale == c2.Scale
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

func DefFromSQLTypes(typs []*sql.ColumnType) (Table, error) {
	ret := Table{}
	for _, t := range typs {
		typ, err := ColumnGoType(t)
		if err != nil {
			return Table{}, err
		}
		sz := int64(0)
		if n, ok := t.Length(); ok {
			sz = n
		}
		nullable := false
		if typ.Kind() == reflect.Ptr {
			nullable = true
			typ = typ.Elem()
		}
		ret = ret.WithColumns(Col{
			Name:     t.Name(),
			Type:     typ,
			Nullable: nullable,
			Length:   sz,
		})
	}
	return ret, nil
}

// DefFromRows scans a slice of rows and returns a TableDef with the columns.
func DefFromRows(rows []Row) (Table, error) {
	it := etl.Values(rows...)
	return FromIterRows(it)
}

func FromIterRows(it etl.Iter) (Table, error) {
	const r = 10
	def := Table{}
	sizes := map[string]int64{}
	updSize := func(name string, l int64) int64 {
		s := l
		if l > r {
			s = (l/r)*r + r
		}
		if s > sizes[name] {
			sizes[name] = s
		}
		return s
	}
	err := etl.Consume(it, func(row drow.Row) error {
		for _, f := range row {
			size := int64(0)
			scale := 0
			switch v := f.Value.(type) {
			case *string:
				if v == nil {
					continue
				}
				l := int64(len(*v))
				size = updSize(f.Name, l)
			case string:
				// round to multiple of r
				l := int64(len(v))
				size = updSize(f.Name, l)
			case *apd.Decimal:
				if v != nil {
					scale = int(v.Exponent)
				}
			case apd.Decimal:
				scale = int(v.Exponent)
			}
			typ := reflect.TypeOf(f.Value)
			nullable := false
			if typ.Kind() == reflect.Ptr {
				nullable = true
				typ = typ.Elem()
			}
			def = def.WithColumns(Col{
				Name:     strings.ToLower(f.Name),
				Type:     typ,
				Nullable: nullable,
				Length:   size,
				Scale:    scale,
			})
		}
		return nil
	})
	return def, err
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

func (d Table) WithColumns(col ...Col) Table {
	clone := Table{
		Columns: append([]Col{}, d.Columns...),
	}
	for _, c := range col {
		i := clone.IndexOf(c.Name)
		if i == -1 {
			clone.Columns = append(clone.Columns, c)
			continue
		}
		// If existing column type is nil, set it to the new one else
		// the original prevails
		if clone.Columns[i].Type == nil && c.Type != nil {
			clone.Columns[i].Type = c.Type
		}
	}
	return clone
}

// MissingOn returns a TableDef with missing columns from d2
func (d Table) MissingOn(d2 Table) Table {
	var ret Table
	for _, c := range d.Columns {
		if d2.IndexOf(c.Name) == -1 {
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

func (d Table) IndexOf(k string) int {
	for i, c := range d.Columns {
		if c.Name == k {
			return i
		}
	}
	return -1
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

func indirect(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}
