package etlparquet

import (
	"bytes"
	"context"
	"fmt"
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
	return etl.MapYield(it, func(p string, yield etl.Y[T]) error {
		defer it.Close()
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		pr, err := goparquet.NewFileReader(f)
		if err != nil {
			return err
		}
		def := pr.GetSchemaDefinition()
		fr := floor.NewReader(pr)
		for fr.Next() {
			var v T
			switch any(v).(type) {
			case drow.Row:
				du := &drowUnmarshaler{def, nil}
				if err := fr.Scan(du); err != nil {
					return err
				}
				v = any(du.row).(T)
			default:
				if err := fr.Scan(&v); err != nil {
					return err
				}
			}
			if err := yield(v); err != nil {
				return err
			}
		}
		return nil
	})
}

type decodeOptions struct {
	useTMPFile bool
}

type decodeOptFunc func(*decodeOptions)

func WithUseTMPFile() decodeOptFunc {
	return func(o *decodeOptions) {
		o.useTMPFile = true
	}
}

func makeDecodeOptions(opts ...decodeOptFunc) decodeOptions {
	opt := decodeOptions{
		useTMPFile: false,
	}
	for _, fn := range opts {
		fn(&opt)
	}
	return opt
}

// Decode decodes and unmarshal []byte from 'iter' into T
func Decode[T any](it Iter, opts ...decodeOptFunc) Iter {
	opt := makeDecodeOptions(opts...)
	return etl.MakeGen(etl.Gen[T]{
		Run: func(ctx context.Context, yield etl.Y[T]) error {
			var dr io.ReadSeeker
			if opt.useTMPFile {
				f, err := os.CreateTemp("", "parquet-")
				if err != nil {
					return err
				}
				defer f.Close()
				defer os.Remove(f.Name())

				if _, err := io.Copy(f, etlio.AsReader(it)); err != nil {
					return err
				}
				dr = f
			} else {
				data, err := etlio.ReadAllContext(ctx, it)
				if err != nil {
					return err
				}
				dr = bytes.NewReader(data)
			}

			pr, err := goparquet.NewFileReader(dr)
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
					du := &drowUnmarshaler{def, nil}
					if err := fr.Scan(du); err != nil {
						return err
					}
					v = any(du.row).(T)
				default:
					if err := fr.Scan(&v); err != nil {
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

// Encode returns a new iterator that will iterate over encoded parquet []byte
// data, it creates the schema based on the first received value.
func Encode(it Iter) Iter {
	runner := func(ctx context.Context, yield etl.Y[[]byte]) error {
		var pw *goparquet.FileWriter
		var fw *floor.Writer
		defer func() {
			if fw != nil {
				fw.Close()
			}
			if pw != nil {
				pw.Close()
			}
		}()
		return etl.ConsumeContext(ctx, it, func(v any) error {
			if pw == nil {
				schema, err := schemaFrom(v)
				if err != nil {
					return err
				}
				w := etlio.YieldWriter(yield)
				pw = goparquet.NewFileWriter(w,
					goparquet.WithSchemaDefinition(schema),
					goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
				)
				fw = floor.NewWriter(pw)
			}
			switch v := v.(type) {
			case drow.Row:
				if err := fw.Write(&drowMarshaler{v}); err != nil {
					return fmt.Errorf("failed to write row: %w", err)
				}
				return nil
			default:
				return fw.Write(v)
			}
		})
	}
	return etl.MakeGen(etl.Gen[[]byte]{
		Run:   runner,
		Close: it.Close,
	})
}

// Build schema definition from reflection
func schemaFrom(v interface{}) (*parquetschema.SchemaDefinition, error) {
	if r, ok := v.(drow.Row); ok {
		return drowSchemaFrom(r)
	}
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
				Name:           t,
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
