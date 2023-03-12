package etlsql

import (
	"context"
	"fmt"

	"github.com/stdiopt/danda/etl"
)

type DDLSync int

const (
	DDLNone DDLSync = iota
	DDLCreate
	DDLAddColumns
)

type insertOptions struct {
	batchSize    int
	ddlSync      DDLSync
	nullables    map[string]struct{}
	typeOverride func(t ColDef) string
}
type insertOptFunc func(*insertOptions)

// WithBatchSize sets the number of rows to insert in a single batch.
// might still execute multiple inserts.
func WithBatchSize(n int) insertOptFunc {
	return func(o *insertOptions) {
		o.batchSize = n
	}
}

// WithDDLSync sets the DDL sync mode.
func WithDDLSync(ddlSync DDLSync) insertOptFunc {
	return func(o *insertOptions) {
		o.ddlSync = ddlSync
	}
}

// WithNullables sets the columns that can be null.
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

// WithTypeOverride uses the sql type returned by the func for col
// if the func returns an empty string, the default type is used.
func WithTypeOverride(fn func(t ColDef) string) insertOptFunc {
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
	tableDef, err := d.dialect.TableDef(ctx, d.q, table)
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
		def, err := DefFromRows(table, rows)
		if err != nil {
			return err
		}
		for i, c := range def.Columns {
			if opt.typeOverride != nil {
				if t := opt.typeOverride(c); t != "" {
					def.Columns[i].SQLType = t
				}
			}
			if opt.nullables == nil {
				continue
			}
			if _, ok := opt.nullables[c.Name]; ok {
				def.Columns[i].Nullable = true
				continue
			}
			if _, ok := opt.nullables["*"]; ok {
				def.Columns[i].Nullable = true
				continue
			}
			// Override sql type somehow and store it on column
		}

		tx, err := d.q.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback() // nolint: errcheck

		// Exceptional case, we attept to reload the table under the transaction if columns are empty.
		if tableDef.Len() == 0 {
			tableDef, err = d.dialect.TableDef(ctx, tx, table)
			if err != nil {
				return err
			}
		}
		// DDLSync
		if missing := def.MissingOn(tableDef); missing.Len() > 0 {
			var err error
			switch {
			case len(tableDef.Columns) == 0:
				if opt.ddlSync < DDLCreate {
					return fmt.Errorf("etlsql.DB.Insert: table '%s' does not exists", table)
				}
				err = d.dialect.CreateTable(ctx, tx, def)
			case opt.ddlSync == DDLAddColumns:
				err = d.dialect.AddColumns(ctx, tx, missing)
			}
			if err != nil {
				return err
			}
			tableDef = def
		}
		rows = tableDef.NormalizeRows(rows)

		if err := d.dialect.Insert(ctx, tx, tableDef, rows); err != nil {
			return err
		}

		return tx.Commit()
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
