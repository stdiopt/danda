package etlsql

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/stdiopt/danda/drow"
)

type Type int

const (
	TypeUnknown          Type = iota
	TypeSmallInt              // int8
	TypeUnsignedSmallInt      // uint8
	TypeInteger               // int32
	TypeUnsignedInteger       // uint32
	TypeBigInt                // int64
	TypeUnsignedBigInt        // uint64
	TypeDecimal               // apd.Decimal
	TypeReal                  // float32
	TypeDouble                // float64
	TypeVarchar               // string
	TypeTimestamp             // time.Time
	TypeBoolean               // bool
	// TypeDate                         // time.Time
	// TypeTime                         // time.Time
	// TypeBlob                         // []byte
)

func (t Type) String() string {
	switch t {
	case TypeUnknown:
		return "unknown"
	case TypeSmallInt:
		return "smallint"
	case TypeUnsignedSmallInt:
		return "unsigned smallint"
	case TypeInteger:
		return "integer"
	case TypeUnsignedInteger:
		return "unsigned integer"
	case TypeBigInt:
		return "bigint"
	case TypeUnsignedBigInt:
		return "unsigned bigint"
	case TypeDecimal:
		return "decimal"
	case TypeReal:
		return "real"
	case TypeDouble:
		return "double"
	case TypeVarchar:
		return "varchar"
	case TypeTimestamp:
		return "timestamp"
	case TypeBoolean:
		return "boolean"
	}
	return "unknown"
}

// ColDef represets a column in a table
type ColDef struct {
	Name string
	// Type     reflect.Type // RAW? ScanType
	Type     Type
	Nullable bool
	Length   int64 // for varchar and maybe other types
	Scale    int   // Precision int ...
	// Overrides for sql types
	SQLType string // override
}

// Eq compares two columns by name and type.
func (c ColDef) Eq(c2 ColDef) bool {
	return c.Name == c2.Name &&
		c.Type == c2.Type &&
		c.Length == c2.Length &&
		c.Scale == c2.Scale
}

// Zero returns the zero value for the column type.
func (c ColDef) Zero() any {
	switch c.Type {
	case TypeSmallInt:
		return int8(0)
	case TypeInteger:
		return int32(0)
	case TypeBigInt:
		return int64(0)
	case TypeDecimal:
		return apd.New(0, int32(c.Scale))
	case TypeReal:
		return float32(0)
	case TypeDouble:
		return float64(0)
	case TypeVarchar:
		return ""
	case TypeTimestamp:
		return time.Time{}
	case TypeBoolean:
		return false
		// case TypeBlob:
		//	return []byte{}
	}
	return nil
}

// TableDef represents an sql table definition.
type TableDef struct {
	Columns []ColDef
}

func NewTableDef(cols ...ColDef) TableDef {
	return TableDef{
		Columns: append([]ColDef{}, cols...),
	}
}

type sqlTypeSolver func(*sql.ColumnType) (reflect.Type, error)

func DefFromSQLTypes(typs []*sql.ColumnType, solverOpt ...sqlTypeSolver) (TableDef, error) {
	var solver sqlTypeSolver
	solver = ColumnGoTypeDef
	if len(solverOpt) > 0 {
		solver = solverOpt[0]
	}
	cols := []ColDef{}
	for _, t := range typs {
		typ, err := solver(t)
		if err != nil {
			return TableDef{}, err
		}
		sz := int64(0)
		if n, ok := t.Length(); ok {
			sz = n
		}
		nullable := false
		if typ.Kind() == reflect.Ptr {
			nullable = true
		}
		styp := typFromGo(typ)
		cols = append(cols, ColDef{
			Name:     t.Name(),
			Type:     styp,
			Nullable: nullable,
			Length:   sz,
		})
	}
	ret := TableDef{
		Columns: cols,
	}
	return ret, nil
}

// Len returns the number of columns in the table.
func (d TableDef) Len() int {
	return len(d.Columns)
}

// Get returns the column with the given name or an empty col if non existent.
func (d TableDef) Get(colName string) (ColDef, bool) {
	for _, c := range d.Columns {
		if c.Name == colName {
			return c, true
		}
	}
	return ColDef{}, false
}

func (d TableDef) WithColumns(col ...ColDef) TableDef {
	clone := TableDef{
		Columns: append([]ColDef{}, d.Columns...),
	}
	for _, c := range col {
		i := clone.IndexOf(c.Name)
		if i == -1 {
			clone.Columns = append(clone.Columns, c)
			continue
		}
		// If existing column type is nil, set it to the new one else
		// the original prevails
		if clone.Columns[i].Type == TypeUnknown && c.Type != TypeUnknown {
			clone.Columns[i].Type = c.Type
		}
		if clone.Columns[i].Length < c.Length {
			clone.Columns[i].Length = c.Length
		}
	}
	return clone
}

// MissingOn returns a TableDef with missing columns from d2
func (d TableDef) MissingOn(d2 TableDef) TableDef {
	ret := TableDef{}
	for _, c := range d.Columns {
		if d2.IndexOf(c.Name) == -1 {
			ret.Columns = append(ret.Columns, c)
		}
	}
	return ret
}

// StrJoin returns a string with all column names joined by sep.
func (d TableDef) StrJoin(sep string) string {
	buf := bytes.Buffer{}
	for i, c := range d.Columns {
		if i != 0 {
			buf.WriteString(sep)
		}
		buf.WriteString(c.Name)
	}
	return buf.String()
}

func (d TableDef) String() string {
	buf := &bytes.Buffer{}
	for _, c := range d.Columns {
		nl := "null"
		if !c.Nullable {
			nl = "not null"
		}
		sz := ""
		if c.Length > 0 {
			sz = fmt.Sprintf("(%d)", c.Length)
		}
		fmt.Fprintf(buf, "  %s %s%s %s\n", c.Name, c.Type, sz, nl)
	}
	return buf.String()
}

func (d TableDef) IndexOf(k string) int {
	for i, c := range d.Columns {
		if strings.EqualFold(c.Name, k) {
			return i
		}
	}
	return -1
}

// Drow related

// NormalizeRows returns a slice of rows based on definition d.
func (d TableDef) NormalizeRows(rows []Row) []Row {
	ret := make([]Row, 0, len(rows))
	for _, r := range rows {
		fields := make([]drow.Field, len(d.Columns))
		for i, c := range d.Columns {
			f := r.At(equalFold(c.Name))
			fields[i] = drow.F(c.Name, f.Value)
		}
		ret = append(ret, Row(fields))
	}
	return ret
}

// RowValues returns a slice of values from the given rows.
// |row1|row2|row3| => |row1[0]|row1[1]|row2[0]|row2[1]|row3[0]|row3[1]|
func (d TableDef) RowValues(rows []Row) []any {
	params := []any{}
	for _, r := range rows {
		for _, c := range d.Columns {
			f := r.At(equalFold(c.Name))
			v := f.Value
			if !c.Nullable && v == nil {
				v = c.Zero()
			}
			params = append(params, v)
		}
	}
	return params
}

func typFromGo(v any) Type {
	var typ reflect.Type
	if t, ok := v.(reflect.Type); ok {
		typ = t
	} else {
		typ = reflect.TypeOf(v)
	}
	switch typ.Kind() {
	case reflect.Ptr:
		return typFromGo(typ.Elem())
	case reflect.Uint8:
		return TypeUnsignedSmallInt
	case reflect.Int8:
		return TypeSmallInt
	case reflect.Uint16:
		return TypeUnsignedInteger
	case reflect.Int16:
		return TypeInteger
	case reflect.Uint32:
		return TypeUnsignedInteger
	case reflect.Int32:
		return TypeInteger
	case reflect.Uint64:
		return TypeUnsignedBigInt
	case reflect.Int64:
		return TypeBigInt
	case reflect.Int:
		return TypeInteger
	case reflect.Float32:
		return TypeReal
	case reflect.Float64:
		return TypeDouble
	case reflect.String:
		return TypeVarchar
	case reflect.Bool:
		return TypeBoolean
	case reflect.Struct:
		if typ == reflect.TypeOf(time.Time{}) {
			return TypeTimestamp
		}
		if typ == reflect.TypeOf(apd.Decimal{}) {
			return TypeDecimal
		}
	default:
		log.Println("Unknown type", typ)
		return TypeUnknown
	}
	return TypeUnknown
}
