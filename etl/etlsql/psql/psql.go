package psql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl/etlsql"
	"golang.org/x/sync/errgroup"
)

type (
	Row      = drow.Row
	TableDef = etlsql.TableDef
	ColDef   = etlsql.ColDef
)

var Dialect = psql{}

type psql struct{}

func (psql) String() string { return "psql" }

func (d psql) TableDef(ctx context.Context, q etlsql.SQLQuery, name string) (TableDef, error) {
	tableQry := `
		SELECT count(table_name)
		FROM information_schema.tables
		WHERE table_schema = 'public'
			AND table_name = $1`
	row := q.QueryRowContext(ctx, tableQry, name)
	var c int
	if err := row.Scan(&c); err != nil {
		return TableDef{}, err
	}
	if c == 0 {
		return TableDef{}, nil
	}

	// Load table schema here
	qry := fmt.Sprintf("SELECT * FROM %s LIMIT 0", name)
	rows, err := q.QueryContext(ctx, qry)
	if err != nil {
		return TableDef{}, fmt.Errorf("selecting table: %w", err)
	}
	defer rows.Close()

	typs, err := rows.ColumnTypes()
	if err != nil {
		return TableDef{}, fmt.Errorf("fetch columns: %w", err)
	}

	return etlsql.DefFromSQLTypes(name, typs, d.ColumnGoType)
}

func (d psql) CreateTable(ctx context.Context, q etlsql.SQLExec, def TableDef) error {
	// Create statement
	params := []any{}
	qry := &bytes.Buffer{}
	fmt.Fprintf(qry, "CREATE TABLE IF NOT EXISTS \"%s\" (\n", def.Name)
	for i, c := range def.Columns {
		sqlType, err := d.columnSQLTypeName(c)
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
		return fmt.Errorf("psql: createTable failed: %w: %v", err, qry.String())
	}
	return nil
}

func (d psql) AddColumns(ctx context.Context, q etlsql.SQLExec, def TableDef) error {
	if len(def.Columns) == 0 {
		return nil
	}

	for _, col := range def.Columns {
		sqlType, err := d.columnSQLTypeName(col)
		if err != nil {
			return fmt.Errorf("field '%s' %w", col.Name, err)
		}

		// in this case we allow null since we're adding a column
		qry := fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN "%s" %s`,
			def.Name,
			col.Name, sqlType,
		)

		_, err = q.ExecContext(ctx, qry)
		if err != nil {
			return fmt.Errorf("addColumns failed: %w: %s", err, qry)
		}
	}
	return nil
}

func (d psql) Insert(ctx context.Context, db etlsql.SQLExec, def TableDef, rows []etlsql.Row) error {
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
			return d.insert(ctx, db, def, rows[offs:end])
		})
	}
	return eg.Wait()
}

func (d psql) ColumnGoType(ct *sql.ColumnType) (reflect.Type, error) {
	switch ct.DatabaseTypeName() {
	case "NUMERIC":
		return apdDecimalTyp, nil
	default:
		return etlsql.ColumnGoTypeDef(ct)
	}
}

func (d psql) insert(ctx context.Context, q etlsql.SQLExec, def TableDef, rows []etlsql.Row) error {
	qryBuf := &bytes.Buffer{}
	insQ := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES ", def.Name, def.StrJoin(", "))
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

func (d *psql) columnSQLTypeName(c ColDef) (string, error) {
	if c.SQLType != "" {
		return c.SQLType, nil
	}

	ftyp := c.Type

	nullable := c.Nullable

	var sqlType string
	var def string
	switch c.Type {
	case etlsql.TypeBoolean:
		sqlType, def = "boolean", "DEFAULT false"
	case etlsql.TypeSmallInt:
		sqlType, def = "smallint", "DEFAULT 0"
	case etlsql.TypeUnsignedSmallInt:
		sqlType, def = "unsigned smallint", "DEFAULT 0"
	case etlsql.TypeInteger:
		sqlType, def = "integer", "DEFAULT 0"
	case etlsql.TypeUnsignedInteger:
		sqlType, def = "unsigned integer", "DEFAULT 0"
	case etlsql.TypeBigInt:
		sqlType, def = "bigint", "DEFAULT 0"
	case etlsql.TypeUnsignedBigInt:
		sqlType, def = "unsigned bigint", "DETAULT 0"
	case etlsql.TypeReal:
		sqlType, def = "real", "DEFAULT 0.0"
	case etlsql.TypeDouble:
		sqlType, def = "double precision", "DEFAULT 0.0"
	case etlsql.TypeVarchar:
		def = "DEFAULT ''"
		sqlType = "varchar"
		if c.Length > 0 {
			sqlType = fmt.Sprintf("varchar(%d)", c.Length)
		}
	case etlsql.TypeTimestamp:
		nullable = true
		sqlType = "timestamp"
	case etlsql.TypeDecimal:
		def = "DEFAULT 0.0"
		sqlType = "decimal"
		if c.Scale != 0 {
			sqlType += fmt.Sprintf("(10,%d)", c.Scale)
		}
	}

	if sqlType == "" {
		return "", fmt.Errorf("dialect.psql: unsupported type: %v", ftyp)
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
