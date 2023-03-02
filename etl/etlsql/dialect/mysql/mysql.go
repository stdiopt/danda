package mysql

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
)

type (
	Row   = drow.Row
	Table = dialect.Table
)

var Dialect = &mysql{}

type mysql struct{}

func (d *mysql) TableDef(ctx context.Context, db etlsql.SQLQuery, name string) (Table, error) {
	tableQry := fmt.Sprintf(`
			SELECT count(table_name) 
			FROM information_schema.tables 
			WHERE table_schema = 'DATABASE()' 
				AND table_name = '%s'`,
		name,
	)
	row := db.QueryRowContext(ctx, tableQry)
	var c int
	if err := row.Scan(&c); err != nil {
		return Table{}, err
	}
	if c == 0 {
		return Table{}, nil
	}

	// Load table schema here
	qry := fmt.Sprintf("SELECT * FROM %s LIMIT 0", name)
	rows, err := db.QueryContext(ctx, qry)
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

func (d *mysql) CreateTable(ctx context.Context, db etlsql.SQLExec, name string, def dialect.Table) error {
	// Create statement
	params := []any{}
	qry := &bytes.Buffer{}
	fmt.Fprintf(qry, "CREATE TABLE IF NOT EXISTS `%s` (\n", name)
	for i, c := range def.Columns {
		sqlType, err := d.columnSQLTypeName(c.Type)
		if err != nil {
			return fmt.Errorf("field '%s' %w", c.Name, err)
		}

		fmt.Fprintf(qry, "\t`%s` %s", c.Name, sqlType)
		if i < len(def.Columns)-1 {
			qry.WriteRune(',')
		}
		qry.WriteRune('\n')
	}
	qry.WriteString(")\n")

	_, err := db.ExecContext(ctx, qry.String(), params...)
	if err != nil {
		return fmt.Errorf("createTable failed: %w: %v", err, qry.String())
	}
	return nil
}

func (p *mysql) AddColumns(ctx context.Context, db etlsql.SQLExec, name string, def dialect.Table) error {
	if len(def.Columns) == 0 {
		return nil
	}
	for _, col := range def.Columns {
		ftyp := col.Type
		sqlType, err := p.columnSQLTypeName(ftyp)
		if err != nil {
			return fmt.Errorf("field '%s' %w", col.Name, err)
		}

		// in this case we allow null since we're adding a column
		qry := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s",
			name,
			col.Name, sqlType,
		)

		_, err = db.ExecContext(ctx, qry)
		if err != nil {
			return fmt.Errorf("addColumns failed: %w", err)
		}
	}
	return nil
}

func (p *mysql) Insert(ctx context.Context, db etlsql.SQLExec, name string, rows []etlsql.Row) error {
	def := dialect.DefFromRows(rows)

	qryBuf := &bytes.Buffer{}
	fmt.Fprintf(qryBuf, "INSERT INTO `%s` (%s) VALUES ", name, def.StrJoin(", "))
	for i := 0; i < len(rows); i++ {
		if i != 0 {
			qryBuf.WriteString("),\n")
		}
		qryBuf.WriteString("(")
		for ri := range def.Columns {
			if ri > 0 {
				qryBuf.WriteString(", ")
			}
			qryBuf.WriteString("?")
		}
	}
	qryBuf.WriteString(")")
	//
	params := def.RowValues(rows)

	_, err := db.ExecContext(ctx, qryBuf.String(), params...)
	return err
}

var (
	timeTyp       = reflect.TypeOf(time.Time{})
	apdDecimalTyp = reflect.TypeOf(apd.Decimal{})
)

func (d mysql) columnSQLTypeName(t reflect.Type) (string, error) {
	ftyp := t

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
		// sqlType = "varchar(max)"
		sqlType, def = "text", ""
		nullable = true
	case reflect.Struct:
		switch ftyp {
		case timeTyp:
			nullable = true
			// sqlType,def = "datetime","01-01-1970 00:00:00"
			sqlType = "datetime"
		case apdDecimalTyp:
			sqlType = "decimal"
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
