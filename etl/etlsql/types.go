package etlsql

import (
	"database/sql"
	"reflect"
	"time"
)

var (
	sqlNullBool    = reflect.TypeOf(sql.NullBool{})
	sqlNullByte    = reflect.TypeOf(sql.NullByte{})
	sqlNullFloat64 = reflect.TypeOf(sql.NullFloat64{})
	sqlNullInt16   = reflect.TypeOf(sql.NullInt16{})
	sqlNullInt32   = reflect.TypeOf(sql.NullInt32{})
	sqlNullInt64   = reflect.TypeOf(sql.NullInt64{})
	sqlNullString  = reflect.TypeOf(sql.NullString{})
	sqlNullTime    = reflect.TypeOf(sql.NullTime{})

	sqlRawBytes = reflect.TypeOf(sql.RawBytes{})

	boolTyp    = reflect.TypeOf(bool(false))
	byteTyp    = reflect.TypeOf(byte(0))
	float64Typ = reflect.TypeOf(float64(0))
	int16Typ   = reflect.TypeOf(int16(0))
	int32Typ   = reflect.TypeOf(int32(0))
	int64Typ   = reflect.TypeOf(int64(0))
	timeTyp    = reflect.TypeOf(time.Time{})
	stringTyp  = reflect.TypeOf("")

	// apdDecimalTyp = reflect.TypeOf(apd.Decimal{})
)

func ColumnGoType(ct *sql.ColumnType) (reflect.Type, error) {
	t := ct.ScanType()
	switch t {
	case sqlNullBool:
		return reflect.PtrTo(boolTyp), nil
	case sqlNullByte:
		return reflect.PtrTo(byteTyp), nil
	case sqlNullFloat64:
		return reflect.PtrTo(float64Typ), nil
	case sqlNullInt16:
		return reflect.PtrTo(int16Typ), nil
	case sqlNullInt32:
		return reflect.PtrTo(int32Typ), nil
	case sqlNullInt64:
		return reflect.PtrTo(int64Typ), nil
	case sqlNullString:
		return reflect.PtrTo(stringTyp), nil
	case sqlNullTime:
		return reflect.PtrTo(timeTyp), nil
	case sqlRawBytes:
		return reflect.PtrTo(stringTyp), nil
	}
	if n, ok := ct.Nullable(); ok && n {
		return reflect.PtrTo(t), nil
	}
	return t, nil
}
