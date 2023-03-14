package etlparquet

import (
	"fmt"
	"log"
	"math/big"
	"reflect"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/util/conv"
)

type drowUnmarshaler struct {
	schema *parquetschema.SchemaDefinition
	row    drow.Row
}

// TODO: improve this.
func (u *drowUnmarshaler) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
	data := obj.GetData()
	for _, ch := range u.schema.RootColumn.Children {
		name := ch.SchemaElement.Name
		v := data[name]
		isNil := false
		// Convert to a typed nil ptr
		if v == nil {
			isNil = true
			switch ch.SchemaElement.GetType() {
			case parquet.Type_BOOLEAN:
				v = (*bool)(nil)
			case parquet.Type_INT32:
				v = (*int32)(nil)
			case parquet.Type_INT64:
				v = (*int64)(nil)
			case parquet.Type_BYTE_ARRAY:
				v = (*[]byte)(nil)
			case parquet.Type_FLOAT:
				v = (*float32)(nil)
			case parquet.Type_DOUBLE:
				v = (*float64)(nil)
			default:
			}
		}

		if ch.SchemaElement.ConvertedType != nil {
			switch ch.SchemaElement.GetConvertedType() {
			case parquet.ConvertedType_INT_8:
				if isNil {
					v = (*int8)(nil)
					break
				}
				v = int8(v.(int32))
			case parquet.ConvertedType_UINT_8:
				if isNil {
					v = (*uint8)(nil)
					break
				}
				v = uint8(v.(int32))
			case parquet.ConvertedType_INT_16:
				if isNil {
					v = (*int16)(nil)
					break
				}
				v = int16(v.(int32))
			case parquet.ConvertedType_UINT_16:
				if isNil {
					v = (*uint16)(nil)
					break
				}
				v = uint16(v.(int32))

			case parquet.ConvertedType_UINT_32:
				if isNil {
					v = (*uint32)(nil)
					break
				}
				v = uint32(v.(int32))
			case parquet.ConvertedType_UINT_64:
				if isNil {
					v = (*uint64)(nil)
					break
				}
				v = uint64(v.(int64))
			case parquet.ConvertedType_UTF8:
				if isNil {
					v = (*string)(nil)
					break
				}
				v = string(v.([]byte))
			case parquet.ConvertedType_TIMESTAMP_MILLIS:
				if isNil {
					v = (*time.Time)(nil)
					break
				}
				v = time.Unix(0, v.(int64)*int64(time.Millisecond))
			case parquet.ConvertedType_TIME_MICROS:
				if isNil {
					v = (*time.Time)(nil)
					break
				}
				v = time.Unix(0, v.(int64)*int64(time.Microsecond))
			case parquet.ConvertedType_TIMESTAMP_MICROS:
				if isNil {
					v = (*time.Time)(nil)
					break
				}
				v = time.Unix(0, v.(int64)*int64(time.Microsecond))
			case parquet.ConvertedType_DECIMAL:
				if isNil {
					// we still need scale here so we can't just send (*apd.Decimal)(nil)
					vv := apd.Decimal{}
					vv.Exponent = ch.SchemaElement.GetScale()
					v = &vv // ;(*apd.Decimal)(nil)
				}
				switch vv := v.(type) {
				case []byte:
					bi := new(big.Int)
					bi.SetBytes(vv)
					a := apd.NewWithBigInt(bi, int32(*ch.SchemaElement.Scale))
					v = a
				}
			case parquet.ConvertedType_DATE:
				if isNil {
					v = (*time.Time)(nil)
					break
				}
				v = time.Unix(0, int64(v.(int32))*int64(time.Hour*24))
			default:
				log.Println("Missing converted:", ch.SchemaElement.GetConvertedType())
			}
		}
		// Ensure that field is a pointer if it is optional
		if rt := ch.SchemaElement.RepetitionType; rt != nil && *rt == parquet.FieldRepetitionType_OPTIONAL {
			typ := reflect.TypeOf(v)
			if typ.Kind() != reflect.Ptr {
				t := reflect.New(typ)
				t.Elem().Set(reflect.ValueOf(v))
				v = t.Interface()
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
		v := f.Value

		v = conv.Deref(v)
		if v == nil {
			continue
		}
		switch v := v.(type) {
		// Maybe reflective way, slower but shorter
		/*
			case *int8:
				if v != nil {
					e.SetInt32(int32(*v))
				}
			case *string:
				if v != nil {
					e.SetByteArray([]byte(*v))
				}
		*/
		case string:
			e.SetByteArray([]byte(v))
		case int8:
			e.SetInt32(int32(v))
		case uint8:
			e.SetInt32(int32(v))
		case int16:
			e.SetInt32(int32(v))
		case uint16:
			e.SetInt32(int32(v))
		case int32:
			e.SetInt32(v)
		case uint32:
			e.SetInt32(int32(v))
		case int:
			e.SetInt32(int32(v))
		case uint:
			e.SetInt32(int32(v))
		case int64:
			e.SetInt64(v)
		case uint64:
			e.SetInt64(int64(v))
		case float32:
			e.SetFloat32(v)
		case float64:
			e.SetFloat64(v)
		case bool:
			e.SetBool(v)
		case time.Time:
			e.SetInt64(v.UnixMilli())
		// case apd.Decimal:
		//	e.SetByteArray(v.Bytes())
		default:
			return fmt.Errorf("MarshalParquet: unsupported type: %T", v)
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
		case reflect.Int8:
			ptyp = parquet.Type_INT32
			convTyp = convType(parquet.ConvertedType_INT_8)
		case reflect.Uint8:
			ptyp = parquet.Type_INT32
			convTyp = convType(parquet.ConvertedType_UINT_8)
		case reflect.Int16:
			ptyp = parquet.Type_INT32
			convTyp = convType(parquet.ConvertedType_INT_16)
		case reflect.Uint16:
			ptyp = parquet.Type_INT32
			convTyp = convType(parquet.ConvertedType_UINT_16)
		case reflect.Int, reflect.Int32:
			ptyp = parquet.Type_INT32
		case reflect.Uint, reflect.Uint32:
			ptyp = parquet.Type_INT32
			convTyp = convType(parquet.ConvertedType_UINT_32)
		case reflect.Int64:
			ptyp = parquet.Type_INT64
		case reflect.Uint64:
			ptyp = parquet.Type_INT64
			convTyp = convType(parquet.ConvertedType_UINT_64)
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
			convTyp = convType(parquet.ConvertedType_UTF8)
			logTyp = &parquet.LogicalType{
				STRING: &parquet.StringType{},
			}
		// Add APD
		case reflect.Struct:
			switch {
			case ityp == reflect.TypeOf(time.Time{}):
				ptyp = parquet.Type_INT64
				convTyp = convType(parquet.ConvertedType_TIMESTAMP_MILLIS)
			case ityp == reflect.TypeOf(apd.Decimal{}):
				ptyp = parquet.Type_BYTE_ARRAY
				convTyp = convType(parquet.ConvertedType_DECIMAL)
			default:
				return nil, fmt.Errorf("etlparquet Unmarshal: unsupported type %v", ityp)
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

func convType(t parquet.ConvertedType) *parquet.ConvertedType {
	return &t
}
