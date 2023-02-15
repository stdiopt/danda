package dialect

import (
	"database/sql"
	"reflect"
	"time"
)

var (
	sqlNullBool   = reflect.TypeOf(sql.NullBool{})
	sqlNullString = reflect.TypeOf(sql.NullString{})
	sqlNullInt64  = reflect.TypeOf(sql.NullInt64{})
	sqlNullTime   = reflect.TypeOf(sql.NullTime{})
	sqlRawBytes   = reflect.TypeOf(sql.RawBytes{})

	boolTyp   = reflect.TypeOf(bool(false))
	int64Typ  = reflect.TypeOf(int64(0))
	timeTyp   = reflect.TypeOf(time.Time{})
	stringTyp = reflect.TypeOf("")

	// apdDecimalTyp = reflect.TypeOf(apd.Decimal{})
)

func ColumnGoType(ct *sql.ColumnType) (reflect.Type, error) {
	t := ct.ScanType()
	switch t {
	case sqlNullBool:
		return reflect.PtrTo(boolTyp), nil
	case sqlNullString:
		return reflect.PtrTo(stringTyp), nil
	case sqlNullInt64:
		return reflect.PtrTo(int64Typ), nil
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
