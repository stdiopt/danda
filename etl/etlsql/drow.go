package etlsql

import (
	"database/sql"
	"reflect"

	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl/etlsql/dialect"
)

func scanRow(rows *sql.Rows, typs []*sql.ColumnType) (Row, error) {
	args := make([]interface{}, len(typs))
	vals := make([]reflect.Value, len(typs))
	for i, t := range typs {
		typ, err := dialect.ColumnGoType(t)
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
	/*
		{ // DEBUG
			for _, a := range args {
				log.Printf("Arg type %T %[1]v:", a)
			}
		}
	*/

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
