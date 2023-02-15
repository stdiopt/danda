package etlsql

import (
	"context"
	"database/sql"

	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/etl/etlsql/dialect"
)

type (
	Row  = drow.Row
	Iter = etl.Iter
)

type Dialect interface {
	TableDef(ctx context.Context, db *sql.DB, name string) (dialect.Table, error)
	CreateTable(ctx context.Context, db *sql.DB, name string, table dialect.Table) error
	AddColumns(ctx context.Context, db *sql.DB, name string, table dialect.Table) error
	Insert(ctx context.Context, db *sql.DB, name string, rows []Row) error
}

type DB struct {
	dialect Dialect
	db      *sql.DB
	err     error
}

func (d DB) DB() *sql.DB {
	return d.db
}

func (d DB) Err() error {
	return d.err
}

// New returns a new DB iterator with the given dialect and database.
func New(d Dialect, db *sql.DB) DB {
	return DB{
		dialect: d,
		db:      db,
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
		db:      db,
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
				rows, err = d.db.QueryContext(ctx, query, args...)
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

			return scanRow(rows, typs)
		},
		Close: func() error {
			if rows == nil {
				return nil
			}
			return rows.Close()
		},
	})
}
