package etlsql

import (
	"database/sql"
	"reflect"
	"strings"

	"github.com/cockroachdb/apd"
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
)

func scanRow(rows *sql.Rows, typs []*sql.ColumnType) (Row, error) {
	args := make([]interface{}, len(typs))
	vals := make([]reflect.Value, len(typs))
	for i, t := range typs {
		typ, err := ColumnGoType(t)
		if err != nil {
			return nil, err
		}
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
		val := reflect.New(typ)
		// New: we pass a pointer to pointer instead of a regular pointer
		// this way we avoid some unwanted nil values
		vals[i] = reflect.New(reflect.PtrTo(typ))
		vals[i].Elem().Set(val)

		args[i] = vals[i].Interface() // vals[i].Interface()
	}
	if err := rows.Scan(args...); err != nil {
		return nil, err
	}

	row := make(Row, len(typs))
	for i, val := range vals {
		var v any
		val = reflect.Indirect(val)
		if !val.IsValid() {
			row[i] = drow.F(typs[i].Name(), v)
			continue
		}
		if val.Kind() == reflect.Ptr && !val.IsZero() {
			v = val.Elem().Interface()
		} else {
			v = val.Interface()
		}
		row[i] = drow.F(typs[i].Name(), v)
	}
	return row, nil
}

// DefFromRows scans a slice of rows and returns a TableDef with the columns.
func DefFromRows(name string, rows []Row) (TableDef, error) {
	it := etl.Values(rows...)
	return DefFromIterRows(name, it)
}

func DefFromIterRows(name string, it etl.Iter) (TableDef, error) {
	def := TableDef{Name: name}
	err := etl.Consume(it, func(row drow.Row) error {
		for _, f := range row {
			name := strings.ToLower(f.Name)

			var col ColDef
			ci := def.IndexOf(name)
			if ci == -1 {
				ci = len(def.Columns)
				col.Name = name
				def.Columns = append(def.Columns, col)
			}
			col = def.Columns[ci]

			switch v := f.Value.(type) {
			case *string:
				if v == nil {
					break
				}
				l := int64(len(*v))
				if col.Length < l {
					col.Length = l
				}
			case string:
				// round to multiple of r
				l := int64(len(v))
				if col.Length < l {
					col.Length = l
				}
			case *apd.Decimal:
				if v != nil {
					col.Scale = int(v.Exponent)
				}
			case apd.Decimal:
				col.Scale = int(v.Exponent)
			}
			if f.Value == nil {
				col.Nullable = true
				def.Columns[ci] = col
				continue
			}
			typ := reflect.TypeOf(f.Value)
			if typ.Kind() == reflect.Ptr {
				col.Nullable = true
			}
			col.Type = typFromGo(typ)
			def.Columns[ci] = col
		}
		return nil
	})
	return def, err
}

// equalFold returns a func used in drow.Row.At to fetch a insesitive case field
func equalFold(s string) func(row Row) *drow.Field {
	return func(row Row) *drow.Field {
		for _, f := range row {
			if strings.EqualFold(f.Name, s) {
				return &f
			}
		}
		return nil
	}
}
