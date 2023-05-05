package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/apd"
	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl/etlsql"
)

type (
	Row   = drow.Row
	Table = etlsql.TableDef
	Col   = etlsql.ColDef
)

var Dialect = mysql{}

type mysql struct{}

func (mysql) String() string { return "mysql" }

func (d mysql) TableDef(ctx context.Context, db etlsql.SQLQuery, dbname, name string) (Table, error) {
	if dbname == "" {
		dbname = "DATABASE()"
	}
	tableQry := `
			SELECT count(table_name)
			FROM information_schema.tables
			WHERE table_schema = ?
				AND table_name = ?`
	row := db.QueryRowContext(ctx, tableQry, dbname, name)
	var c int
	if err := row.Scan(&c); err != nil {
		return Table{}, err
	}
	if c == 0 {
		return Table{}, nil
	}

	// Load table schema here
	var qry string
	if dbname == "" {
		qry = fmt.Sprintf("SELECT * FROM `%s` LIMIT 0", name)
	} else {
		qry = fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 0", dbname, name)
	}

	rows, err := db.QueryContext(ctx, qry)
	if err != nil {
		return Table{}, fmt.Errorf("selecting table: %w", err)
	}
	defer rows.Close()

	typs, err := rows.ColumnTypes()
	if err != nil {
		return Table{}, fmt.Errorf("fetch columns: %w", err)
	}

	return etlsql.DefFromSQLTypes(typs, d.ColumnGoType)
}

func (d mysql) CreateTable(ctx context.Context, db etlsql.SQLExec, dbname, name string, def Table) error {
	// Create statement
	params := []any{}
	qry := &bytes.Buffer{}
	if dbname == "" {
		fmt.Fprintf(qry, "CREATE TABLE IF NOT EXISTS `%s` (\n", name)
	} else {
		fmt.Fprintf(qry, "CREATE TABLE IF NOT EXISTS `%s`.`%s` (\n", dbname, name)
	}
	for i, c := range def.Columns {
		sqlType, err := d.columnSQLTypeName(c)
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

func (d mysql) AddColumns(ctx context.Context, db etlsql.SQLExec, dbn, name string, def Table) error {
	if len(def.Columns) == 0 {
		return nil
	}
	for _, col := range def.Columns {
		sqlType, err := d.columnSQLTypeName(col)
		if err != nil {
			return fmt.Errorf("field '%s' %w", col.Name, err)
		}

		// in this case we allow null since we're adding a column
		var qry string
		if dbn == "" {
			qry = fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s",
				name,
				col.Name, sqlType,
			)
		} else {
			qry = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s",
				dbn, name,
				col.Name, sqlType,
			)
		}

		_, err = db.ExecContext(ctx, qry)
		if err != nil {
			return fmt.Errorf("addColumns failed: %w", err)
		}
	}
	return nil
}

func (d mysql) Insert(ctx context.Context, db etlsql.SQLExec, dbn, name string, def Table, rows []etlsql.Row) error {
	qryBuf := &bytes.Buffer{}
	if dbn == "" {
		fmt.Fprintf(qryBuf, "INSERT INTO `%s` (%s) VALUES ", name, def.StrJoin(", "))
	} else {
		fmt.Fprintf(qryBuf, "INSERT INTO `%s`.`%s` (%s) VALUES ", dbn, name, def.StrJoin(", "))
	}
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

func (d mysql) ColumnGoType(ct *sql.ColumnType) (reflect.Type, error) {
	switch strings.ToUpper(ct.DatabaseTypeName()) {
	// need to tackle this
	// case "BIT":
	//	return byteTyp, nil
	case "DECIMAL":
		return apdDecimalTyp, nil
	default:
		return etlsql.ColumnGoTypeDef(ct)
	}
}

// timeTyp       = reflect.TypeOf(time.Time{})
// boolTyp       = reflect.TypeOf(bool(false))
// byteTyp       = reflect.TypeOf(byte(0))
var apdDecimalTyp = reflect.TypeOf(apd.Decimal{})

func (d mysql) columnSQLTypeName(c Col) (string, error) {
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
		sqlType, def = "unsigned bigint", "DEFAULT 0"
	case etlsql.TypeReal:
		sqlType, def = "float", "DEFAULT 0.0"
	case etlsql.TypeDouble:
		sqlType, def = "double", "DEFAULT 0.0"
	case etlsql.TypeVarchar:
		def = "DEFAULT ''"
		sqlType = "varchar" // defaultSize?
		if c.Length > 0 {
			sqlType = fmt.Sprintf("varchar(%d)", c.Length)
		}
	case etlsql.TypeTimestamp:
		nullable = true
		sqlType = "datetime"
	case etlsql.TypeDecimal:
		sqlType = "decimal"
		if c.Scale != 0 {
			sqlType += fmt.Sprintf("(10,%d)", c.Scale)
		}
		def = "DEFAULT 0.0"
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
