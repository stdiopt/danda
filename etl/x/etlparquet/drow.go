package etlparquet

import (
	"math/big"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/fraugster/parquet-go/floor/interfaces"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stdiopt/danda/drow"
)

type drowUnmarshaller struct {
	schema *parquetschema.SchemaDefinition
	row    drow.Row
}

func (u *drowUnmarshaller) UnmarshalParquet(obj interfaces.UnmarshalObject) error {
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
