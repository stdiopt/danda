package etlparquet

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"time"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlio"
)

type (
	Iter = etl.Iter
	Row  = drow.Row
)

// DecodeFile receives a string path and outputs T.
func DecodeFile[T any](it Iter) Iter {
	return etl.Yield(it, func(p string, yield etl.Y[T]) error {
		defer it.Close()
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		pr, err := goparquet.NewFileReader(f)
		if err != nil {
			return err
		}
		fr := floor.NewReader(pr)
		for fr.Next() {
			v := new(T)
			if err := fr.Scan(v); err != nil {
				return err
			}
			if err := yield(*v); err != nil {
				return err
			}
		}
		return nil
	})
}

// Decode decodes and unmarshal parquet data into T
func Decode[T any](it Iter) Iter {
	return etl.MakeGen(etl.Gen[T]{
		Run: func(yield etl.Y[T]) error {
			data, err := io.ReadAll(etlio.AsReader(it))
			if err != nil {
				return err
			}

			pr, err := goparquet.NewFileReader(bytes.NewReader(data))
			if err != nil {
				return err
			}
			def := pr.GetSchemaDefinition()
			fr := floor.NewReader(pr)
			defer fr.Close()

			for fr.Next() {
				var v T
				switch any(v).(type) {
				case drow.Row:
					du := &drowUnmarshaller{def, nil}
					if err := fr.Scan(du); err != nil {
						return err
					}
					v = any(du.row).(T)
				default:
					if err := fr.Scan(v); err != nil {
						return err
					}
				}
				if err := yield(v); err != nil {
					return err
				}
			}
			return nil
		},
		Close: it.Close,
	})
}

// Encode receives a T and outputs encoded parquet in []byte
// Add drow support
func Encode[T any](it Iter) Iter {
	return etl.MakeGen(etl.Gen[[]byte]{
		Run: func(yield etl.Y[[]byte]) error {
			w := etlio.YieldWriter(yield)
			pw := goparquet.NewFileWriter(w,
				goparquet.WithSchemaDefinition(schemaFrom(*new(T))),
				goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
			)
			defer pw.Close()
			fw := floor.NewWriter(pw)
			defer fw.Close()

			return etl.Consume(it, fw.Write)
		},
		Close: it.Close,
	})
}

// Build schema definition from reflection
func schemaFrom(v interface{}) *parquetschema.SchemaDefinition {
	val := reflect.Indirect(reflect.ValueOf(v))
	typ := val.Type()

	root := &parquetschema.SchemaDefinition{
		RootColumn: &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{Name: "Thing"},
		},
	}

	for i := 0; i < typ.NumField(); i++ {
		ftyp := typ.Field(i)
		t, ok := ftyp.Tag.Lookup("parquet")
		if !ok {
			continue
		}

		var ptyp parquet.Type
		var convTyp *parquet.ConvertedType
		var logTyp *parquet.LogicalType

		rep := parquet.FieldRepetitionType_REQUIRED

		ityp := val.Field(i).Type()
		if val.Field(i).Kind() == reflect.Ptr {
			rep = parquet.FieldRepetitionType_OPTIONAL
			ityp = val.Field(i).Type().Elem()
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
				panic("wrong type")
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
				panic("only type supported for now")
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
				Name:           t,
				Type:           &ptyp,
				RepetitionType: &rep,
				ConvertedType:  convTyp,
				LogicalType:    logTyp,
			},
		}
		root.RootColumn.Children = append(root.RootColumn.Children, col)
	}
	return root
}

// generate schema from iface, using message maybe using Schema struct directly.
/*func withSchema(v interface{}) parquetgo.FileWriterOption {

	log.Printf("For type: %T", v)
	val := reflect.Indirect(reflect.ValueOf(v))
	typ := val.Type()

	sb := &strings.Builder{}
	fmt.Fprintf(sb, "message %s {\n", typ.Name())
	for i := 0; i < typ.NumField(); i++ {
		ftyp := typ.Field(i)
		t, ok := ftyp.Tag.Lookup("parquet")
		if !ok {
			continue
		}
		pty := "binary"
		name := t
		xtra := ""
		vi := val.Field(i).Interface()
		switch vi.(type) {
		case int, int32:
			pty = "int32"
		case int64:
			pty = "int64"
		case string:
			pty = "binary"
			xtra = "(STRING)"
		case time.Time:
			pty = "int64"
			xtra = "(TIMESTAMP(NANOS,true))"
		}
		fmt.Fprintf(sb, "\trequired %s %s %s;\n", pty, name, xtra)
	}
	fmt.Fprint(sb, "}\n")

	log.Println("Res:", sb.String())
	s, err := parquetschema.ParseSchemaDefinition(sb.String())
	if err != nil {
		panic(err)
	}
	return parquetgo.WithSchemaDefinition(s)
}*/
