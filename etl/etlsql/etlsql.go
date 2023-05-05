package etlsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
)

type (
	Row  = drow.Row
	Iter = etl.Iter
)

type SQLQuery interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
type SQLExec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Dialect interface {
	ColumnGoType(ct *sql.ColumnType) (reflect.Type, error)
	TableDef(ctx context.Context, db SQLQuery, schema, name string) (TableDef, error)
	CreateTable(ctx context.Context, db SQLExec, schema, name string, table TableDef) error
	AddColumns(ctx context.Context, db SQLExec, schema, name string, table TableDef) error
	Insert(ctx context.Context, db SQLExec, schema, name string, table TableDef, rows []Row) error
}

type Q interface {
	SQLQuery
	SQLExec
	Begin() (*sql.Tx, error)
}

type DB struct {
	dialect Dialect
	q       Q
	err     error
}

func (d DB) String() string {
	return fmt.Sprintf("dialect: %T", d.dialect)
}

func (d DB) Q() Q { return d.q }

func (d DB) Err() error { return d.err }

// New returns a new DB iterator with the given dialect and database.
func New(d Dialect, db Q) DB {
	return DB{
		dialect: d,
		q:       db,
	}
}

// Open opens a DB connection similar to sql.Open and returns a db Iterator.
func Open(d Dialect, driver, dsn string) DB {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return DB{err: err}
	}

	return DB{
		dialect: d,
		q:       db,
	}
}

// Query executes the given query and returns an iterator that produces drow.Row.
func (d DB) Query(query string, args ...any) Iter {
	if d.err != nil {
		return etl.ErrIter(d.err)
	}
	var rows *sql.Rows
	var typs []*sql.ColumnType
	return etl.MakeIter(etl.Custom[Row]{
		Next: func(ctx context.Context) (Row, error) {
			if rows == nil {
				var err error
				rows, err = d.q.QueryContext(ctx, query, args...)
				if err != nil {
					return nil, err
				}

				typs, err = rows.ColumnTypes()
				if err != nil {
					return nil, err
				}
			}
			if !rows.Next() {
				return nil, etl.EOI
			}

			return d.scanRow(rows, typs)
		},
		Close: func() error {
			if rows == nil {
				return nil
			}
			return rows.Close()
		},
	})
}
