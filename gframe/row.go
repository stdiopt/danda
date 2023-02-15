package gframe

import "github.com/stdiopt/danda/drow"

type (
	// Row is a row of data
	Row = drow.Row
	// Field is a field of data
	Field = drow.Field
	// IntOrString stub interface
	IntOrString = drow.IntOrString
	// FieldExpr for Selects,At, etc..
	FieldExpr = drow.FieldExpr
)

// F returns a new field.
func F(name string, value any) Field { return drow.F(name, value) }

// FF Used to fetch sub fields from a row.
func FE(s ...IntOrString) FieldExpr { return drow.FE(s...) }
