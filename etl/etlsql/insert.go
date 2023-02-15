package etlsql

import (
	"context"
	"fmt"
	"reflect"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlsql/dialect"
)

type DDLSync int

const (
	DDLNone DDLSync = iota
	DDLCreate
	DDLAlter
)

type insertOptions struct {
	batchSize    int
	ddlSync      DDLSync
	nullables    map[string]struct{}
	typeOverride func(t dialect.Col) string
}
type insertOptFunc func(*insertOptions)

func WithBatchSize(n int) insertOptFunc {
	return func(o *insertOptions) {
		o.batchSize = n
	}
}

func WithDDLSync(ddlSync DDLSync) insertOptFunc {
	return func(o *insertOptions) {
		o.ddlSync = ddlSync
	}
}

func WithNullables(nullables ...string) insertOptFunc {
	return func(o *insertOptions) {
		if o.nullables == nil {
			o.nullables = map[string]struct{}{}
		}
		for _, n := range nullables {
			o.nullables[n] = struct{}{}
		}
	}
}

func WithTypeOverride(fn func(t dialect.Col) string) insertOptFunc {
	return func(o *insertOptions) {
		o.typeOverride = fn
	}
}

func (o *insertOptions) apply(opts ...insertOptFunc) {
	for _, fn := range opts {
		fn(o)
	}
}

func (d DB) Insert(it Iter, table string, opts ...insertOptFunc) error {
	if d.err != nil {
		return d.err
	}

	opt := insertOptions{
		batchSize: 1,
		ddlSync:   DDLNone,
	}
	opt.apply(opts...)

	ctx := context.Background()
	tableDef, err := d.dialect.TableDef(ctx, d.db, table)
	if err != nil {
		return err
	}
	// there is no table
	if tableDef.Columns == nil {
		if opt.ddlSync == DDLNone {
			return fmt.Errorf("etlsql.DB.Insert: table '%s' does not exists", table)
		}
	}
	insert := func(ctx context.Context, rows []Row) error {
		if len(rows) == 0 {
			return nil
		}
		def := dialect.FromRows(rows)
		if opt.nullables != nil {
			for i, c := range def.Columns {
				if opt.typeOverride != nil {
					t := opt.typeOverride(c)
					if t != "" {
						def.Columns[i].SQLType = t
					}
				}
				if c.Type.Kind() == reflect.Ptr {
					continue
				}
				if _, ok := opt.nullables[c.Name]; ok {
					def.Columns[i].Type = reflect.PtrTo(c.Type)
					continue
				}
				if _, ok := opt.nullables["*"]; ok {
					def.Columns[i].Type = reflect.PtrTo(c.Type)
					continue
				}
				// Override sql type somehow and store it on column
			}
		}

		// DDLSync
		if missing := def.MissingOn(tableDef); missing.Len() > 0 {
			if len(tableDef.Columns) == 0 {
				if opt.ddlSync < DDLCreate {
					return fmt.Errorf("etlsql.DB.Insert: table '%s' does not exists", table)
				}
				if err := d.dialect.CreateTable(ctx, d.db, table, def); err != nil {
					return err
				}
				tableDef = def
			} else if opt.ddlSync == DDLAlter {
				if err := d.dialect.AddColumns(ctx, d.db, table, missing); err != nil {
					return err
				}
				// Reload def
				tableDef, err = d.dialect.TableDef(ctx, d.db, table)
				if err != nil {
					return err
				}
			}
		}
		rows = tableDef.NormalizeRows(rows)

		return d.dialect.Insert(ctx, d.db, table, rows)
	}

	rows := []Row{}
	return func() (err error) {
		defer func() {
			if err != nil {
				return
			}
			if len(rows) > 0 {
				if ierr := insert(ctx, rows); ierr != nil {
					err = ierr
				}
			}
		}()
		return etl.Consume(it, func(row Row) error {
			rows = append(rows, row)
			if len(rows) < opt.batchSize {
				return nil
			}

			if err := insert(ctx, rows); err != nil {
				return err
			}
			rows = []Row{}
			return nil
		})
	}()
}
