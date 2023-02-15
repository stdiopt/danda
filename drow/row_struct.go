package drow

import (
	"log"
	"reflect"
	"strings"
)

func FromStruct(v any) Row {
	val := reflect.Indirect(reflect.ValueOf(v))
	typ := val.Type()

	row := Row{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Anonymous {
			log.Println("To implement")
			// Copy sub fields to selft
		}
		if !field.IsExported() {
			continue
		}
		if r, ok := val.Field(i).Interface().(Row); ok {
			f := Field{field.Name, r}
			row = append(row, f)
			continue
		}

		if typ.Field(i).Type.Kind() == reflect.Struct {
			f := Field{field.Name, FromStruct(val.Field(i).Interface())}
			row = append(row, f)
			continue
		}

		row = append(row, Field{field.Name, val.Field(i).Interface()})
	}
	return row
}

func XToStruct(r Row) any {
	rtyp := rowTyp(r)
	val := reflect.New(rtyp).Elem()

	for i, f := range r {
		if vv, ok := f.Value.(Row); ok {
			val.Field(i).Set(reflect.ValueOf(XToStruct(vv)))
			continue
		}
		val.Field(i).Set(reflect.ValueOf(f.Value))
	}
	return val.Interface()
}

func rowTyp(r Row) reflect.Type {
	fields := []reflect.StructField{}
	for _, f := range r {
		var t reflect.Type
		if rr, ok := f.Value.(Row); ok {
			t = rowTyp(rr)
		} else {
			t = reflect.TypeOf(f.Value)
		}
		field := reflect.StructField{
			Name: goifyName(f.Name),
			Type: t,
			Tag:  reflect.StructTag(`json:"` + strings.ReplaceAll(f.Name, ",", "_") + `"`),
		}

		fields = append(fields, field)
	}

	return reflect.StructOf(fields)
}

// goifyName any invalid char will be chomped
func goifyName(s string) string {
	return strings.Title(s)
}
