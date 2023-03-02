package psql

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl/etlsql"
	"github.com/stdiopt/danda/etl/etlsql/dialect"
	"golang.org/x/sync/errgroup"
)

type (
	Row   = drow.Row
	Table = dialect.Table
)

var Dialect = &psql{}

type psql struct{}

func (p *psql) TableDef(ctx context.Context, q etlsql.SQLQuery, name string) (Table, error) {
	tableQry := fmt.Sprintf(`
			SELECT count(table_name) 
			FROM information_schema.tables 
			WHERE table_schema = 'public' 
				AND table_name = '%s'`,
		name,
	)
	row := q.QueryRowContext(ctx, tableQry)
	var c int
	if err := row.Scan(&c); err != nil {
		return Table{}, err
	}
	if c == 0 {
		return Table{}, nil
	}

	// Load table schema here
	qry := fmt.Sprintf("SELECT * FROM %s LIMIT 0", name)
	rows, err := q.QueryContext(ctx, qry)
	if err != nil {
		return Table{}, fmt.Errorf("selecting table: %w", err)
	}
	defer rows.Close()

	typs, err := rows.ColumnTypes()
	if err != nil {
		return Table{}, fmt.Errorf("fetch columns: %w", err)
	}

	ret := Table{}
	for _, t := range typs {
		typ, err := dialect.ColumnGoType(t)
		if err != nil {
			return Table{}, err
		}
		ret = ret.WithColumns(dialect.Col{
			Name: t.Name(),
			Type: typ,
		})
	}
	return ret, nil
}

func (p *psql) CreateTable(ctx context.Context, q etlsql.SQLExec, name string, def dialect.Table) error {
	// Create statement
	params := []any{}
	qry := &bytes.Buffer{}
	fmt.Fprintf(qry, "CREATE TABLE IF NOT EXISTS \"%s\" (\n", name)
	for i, c := range def.Columns {
		sqlType, err := p.columnSQLTypeName(c)
		if err != nil {
			return fmt.Errorf("field '%s' %w", c.Name, err)
		}

		fmt.Fprintf(qry, "\t\"%s\" %s", c.Name, sqlType)
		if i < len(def.Columns)-1 {
			qry.WriteRune(',')
		}
		qry.WriteRune('\n')
	}
	qry.WriteString(")\n")

	_, err := q.ExecContext(ctx, qry.String(), params...)
	if err != nil {
		return fmt.Errorf("createTable failed: %w: %v", err, qry.String())
	}
	return nil
}

func (p *psql) AddColumns(ctx context.Context, q etlsql.SQLExec, name string, def dialect.Table) error {
	if len(def.Columns) == 0 {
		return nil
	}

	for _, col := range def.Columns {
		sqlType, err := p.columnSQLTypeName(col)
		if err != nil {
			return fmt.Errorf("field '%s' %w", col.Name, err)
		}

		// in this case we allow null since we're adding a column
		qry := fmt.Sprintf(`
			ALTER TABLE "%s"
			ADD COLUMN "%s" %s`,
			name,
			col.Name, sqlType,
		)

		_, err = q.ExecContext(ctx, qry)
		if err != nil {
			return fmt.Errorf("addColumns failed: %w: %s", err, qry)
		}
	}
	return nil
}

func (p *psql) Insert(ctx context.Context, db etlsql.SQLExec, name string, rows []etlsql.Row) error {
	// some psql engines allows max 64k params per query but to be safe we
	// will use 32k, eventually we can use a config to set this value
	maxParams := 32767

	maxRows := maxParams / len(rows[0])

	eg, ctx := errgroup.WithContext(ctx)
	for offset := 0; offset < len(rows); offset += maxRows {
		end := offset + maxRows
		if end > len(rows) {
			end = len(rows)
		}
		offs := offset
		eg.Go(func() error {
			return p.insert(ctx, db, name, rows[offs:end])
		})
	}
	return eg.Wait()
}

func (p *psql) insert(ctx context.Context, q etlsql.SQLExec, name string, rows []etlsql.Row) error {
	def := dialect.DefFromRows(rows)

	qryBuf := &bytes.Buffer{}
	insQ := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES ", name, def.StrJoin(", "))
	qryBuf.WriteString(insQ)
	pi := 1
	for i := 0; i < len(rows); i++ {
		if i != 0 {
			qryBuf.WriteString("),\n")
		}
		qryBuf.WriteString("(")
		for ri := range def.Columns {
			if ri > 0 {
				qryBuf.WriteString(", ")
			}
			fmt.Fprintf(qryBuf, "$%d", pi)
			pi++
		}
	}
	qryBuf.WriteString(")")

	params := def.RowValues(rows)
	if _, err := q.ExecContext(ctx, qryBuf.String(), params...); err != nil {
		return fmt.Errorf("insert failed: %w, %s", err, insQ)
	}
	return nil
}

var (
	timeTyp       = reflect.TypeOf(time.Time{})
	apdDecimalTyp = reflect.TypeOf(apd.Decimal{})
)

func (p *psql) columnSQLTypeName(c dialect.Col) (string, error) {
	if c.Type == nil {
		return "", fmt.Errorf("nil type")
	}
	if c.SQLType != "" {
		return c.SQLType, nil
	}

	ftyp := c.Type

	nullable := false
	if ftyp.Kind() == reflect.Ptr {
		nullable = true
		ftyp = ftyp.Elem()
	}

	var sqlType string
	var def string
	switch ftyp.Kind() {
	case reflect.Bool:
		sqlType, def = "boolean", "DEFAULT false"
	case reflect.Int, reflect.Int16, reflect.Int32:
		sqlType, def = "integer", "DEFAULT 0"
	case reflect.Uint, reflect.Uint16, reflect.Uint32:
		sqlType, def = "unsigned integer", "DEFAULT 0"
	case reflect.Int64:
		sqlType, def = "bigint", "DEFAULT 0"
	case reflect.Uint64:
		sqlType, def = "unsigned bigint", "DETAULT 0"
	case reflect.Float32, reflect.Float64:
		sqlType, def = "float", "DEFAULT 0.0"
	case reflect.String: // or blob?
		// sqlType, def = "varchar(max)", "DEFAULT ''"
		sqlType, def = "text", "DEFAULT ''"
	case reflect.Struct:
		switch ftyp {
		case timeTyp:
			nullable = true
			// sqlType,def = "datetime","01-01-1970 00:00:00"
			sqlType = "timestamp"
		case apdDecimalTyp:
			sqlType = "decimal"
			if c.Scale != 0 {
				sqlType += fmt.Sprintf("(10,%d)", c.Scale)
			}
			def = "DEFAULT 0.0"
		}
	}

	if sqlType == "" {
		return "", fmt.Errorf("unsupported type: %v", ftyp)
	}

	var e string
	sqlNull := "NULL"
	if !nullable {
		sqlNull = "NOT NULL"
		if def != "" {
			e = def
		}
	}
	return fmt.Sprintf("%s %s %s", sqlType, sqlNull, e), nil
}
