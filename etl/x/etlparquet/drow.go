package etlparquet

import (
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stdiopt/danda/drow"
)

type drowUnmarshaler struct {
	schema *parquetschema.SchemaDefinition
	row    drow.Row
}

func (u *drowUnmarshaler) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	data := obj.GetData()
	for _, ch := range u.schema.RootColumn.Children {
		name := ch.SchemaElement.Name
		v, ok := data[name]
		if !ok {
			continue
		}
		if ch.SchemaElement.ConvertedType != nil {
			switch *ch.SchemaElement.ConvertedType {
			case parquet.ConvertedType_UTF8:
				v = string(v.([]byte))
			case parquet.ConvertedType_TIMESTAMP_MILLIS:
				v = time.Unix(0, v.(int64)*int64(time.Millisecond))
			case parquet.ConvertedType_TIME_MICROS:
				v = time.Unix(0, v.(int64)*int64(time.Microsecond))
			case parquet.ConvertedType_TIMESTAMP_MICROS:
				v = time.Unix(0, v.(int64)*int64(time.Microsecond))
			case parquet.ConvertedType_DECIMAL:
				switch vv := v.(type) {
				case []byte:
					bi := new(big.Int)
					bi.SetBytes(vv)
					a := apd.NewWithBigInt(bi, int32(*ch.SchemaElement.Scale))
					v = a
				}
			}
		}
		u.row = u.row.WithField(name, v)
	}
	return nil
}

type drowMarshaler struct {
	row drow.Row
}

func (m *drowMarshaler) MarshalParquet(obj interfaces.MarshalObject) error {
	for _, f := range m.row {
		e := obj.AddField(f.Name)
		switch v := f.Value.(type) {
		// Maybe reflective way, slower but shorter
		case *string:
			if v != nil {
				e.SetByteArray([]byte(*v))
			}
		case string:
			e.SetByteArray([]byte(v))
		case int8:
			e.SetInt32(int32(v))
		case int16:
			e.SetInt32(int32(v))
		case int32:
			e.SetInt32(v)
		case int64:
			e.SetInt64(v)
		case int:
			e.SetInt32(int32(v))
		case uint8:
			e.SetInt32(int32(v))
		case uint16:
			e.SetInt32(int32(v))
		case uint32:
			e.SetInt32(int32(v))
		case uint64:
			e.SetInt64(int64(v))
		case uint:
			e.SetInt32(int32(v))
		case float32:
			e.SetFloat32(v)
		case float64:
			e.SetFloat64(v)
		case bool:
			e.SetBool(v)
		case time.Time:
			e.SetInt64(v.UnixNano())
		default:
			return fmt.Errorf("unsupported type: %T", v)
		}
	}
	return nil
}

func drowSchemaFrom(r drow.Row) (*parquetschema.SchemaDefinition, error) {
	root := &parquetschema.SchemaDefinition{
		RootColumn: &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{},
		},
	}
	for _, f := range r {
		ftyp := reflect.TypeOf(f.Value)

		var ptyp parquet.Type
		var convTyp *parquet.ConvertedType
		var logTyp *parquet.LogicalType

		rep := parquet.FieldRepetitionType_REQUIRED

		ityp := ftyp
		if ityp.Kind() == reflect.Ptr {
			rep = parquet.FieldRepetitionType_OPTIONAL
			ityp = ityp.Elem()
		}
		switch ityp.Kind() {
		case reflect.Int, reflect.Int32:
			ptyp = parquet.Type_INT32
		case reflect.Int64:
			ptyp = parquet.Type_INT64
		case reflect.Float32:
			ptyp = parquet.Type_FLOAT
		case reflect.Float64:
			ptyp = parquet.Type_DOUBLE
		case reflect.String:
			ptyp = parquet.Type_BYTE_ARRAY
			convTyp = new(parquet.ConvertedType)
			*convTyp = parquet.ConvertedType_UTF8
			logTyp = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
		case reflect.Slice:
			if ityp.Elem().Kind() != reflect.Uint8 {
				return nil, fmt.Errorf("unsupported type %v", ityp)
			}

			ptyp = parquet.Type_BYTE_ARRAY
			convTyp = new(parquet.ConvertedType)
			*convTyp = parquet.ConvertedType_UTF8
			logTyp = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
			// regular slice
		case reflect.Struct:
			if ityp != reflect.TypeOf(time.Time{}) {
				return nil, fmt.Errorf("unsupported type %v", ityp)
			}
			ptyp = parquet.Type_INT64
			logTyp = &parquet.LogicalType{
				TIMESTAMP: &parquet.TimestampType{
					IsAdjustedToUTC: true,
					Unit: &parquet.TimeUnit{
						NANOS: &parquet.NanoSeconds{},
					},
				},
			}
		}
		col := &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name:           f.Name,
				Type:           &ptyp,
				RepetitionType: &rep,
				ConvertedType:  convTyp,
				LogicalType:    logTyp,
			},
		}
		root.RootColumn.Children = append(root.RootColumn.Children, col)
	}
	return root, nil
}
