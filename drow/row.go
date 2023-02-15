// Package drow provides a dynamic struct/row type.
package drow

import (
	"bytes"
	"fmt"
	"strings"
)

type IntOrString any

// Row is a slice of fields with a defined sequence.
type Row []Field

// FromMap creates a row from a map[string]interface{}
// the fields might not be in the same order as the map
func FromMap(m map[string]any) Row {
	r := Row{}
	for k, v := range m {
		r = append(r, F(k, v))
	}
	return r
}

// ToMap converts a row to map[string]any
func (r Row) ToMap() map[string]any {
	m := map[string]any{}
	for _, f := range r {
		if sub, ok := f.Value.(Row); ok {
			m[f.Name] = sub.ToMap()
			continue
		}
		m[f.Name] = f.Value
	}
	return m
}

// Eq returns true if the v is equal to row r
func (r Row) Eq(v any) bool {
	r2, ok := v.(Row)
	if !ok {
		return false
	}
	if len(r) != len(r2) {
		return false
	}
	for i := 0; i < len(r); i++ {
		b := func() (ret bool) {
			// Recover from non comparables
			defer func() {
				if p := recover(); p != nil {
					ret = false
					// Repanic with better info
					// panic(fmt.Sprintf("error comparing field: %v: %v", r1[i].Name, p))
				}
			}()
			return r[i] == r2[i]
		}()

		if !b {
			return false
		}
	}
	return true
}

func (r Row) String() string {
	buf := bytes.NewBuffer(nil)
	fmt.Fprintf(buf, "{")
	for i, f := range r {
		if i > 0 {
			fmt.Fprint(buf, ", ")
		}
		fmt.Fprintf(buf, "%s: %v", f.Name, f.Value)
	}
	fmt.Fprintf(buf, "}")
	return buf.String()
}

// Should deep clone?
func (r Row) clone() Row {
	return append(Row{}, r...)
}

// Columns returns the names of the fields.
func (r Row) Columns() []string {
	ret := make([]string, len(r))
	for i, f := range r {
		ret[i] = f.Name
	}
	return ret
}

// Values return a []any of the values of the row fields.
func (r Row) Values() []any {
	ret := make([]any, len(r))
	for i, f := range r {
		ret[i] = f.Value
	}
	return ret
}

// Flat if a field is a Row it's fields will be in the main row.
func (r Row) Flat() Row {
	ret := Row{}
	for _, f := range r {
		if v, ok := f.Value.(Row); ok {
			for _, ff := range v.Flat() {
				ret = append(ret, F(f.Name+"."+ff.Name, ff.Value))
			}
			continue
		}
		ret = append(ret, f)
	}
	return ret
}

// Value returns the value at s
func (r Row) Value(s IntOrString) any {
	f := r.at(s)
	if f == nil {
		return nil
	}
	return f.Value
}

// At returns the value of a field named s.
func (r Row) At(s IntOrString) Field {
	f := r.at(s)
	if f == nil {
		return Field{}
	}
	return *f
}

// Has returns true if the row has a field named s, false otherwise.
func (r Row) Has(s string) bool {
	for _, f := range r {
		if f.Name == s {
			return true
		}
	}
	return false
}

// Merge will set the fields of r2 in r.
func (r Row) Merge(r2 Row) Row {
	nr := r.clone()
	for _, f := range r2 {
		nr.set(f.Name, f.Value)
	}
	return nr
}

// Concat returns a new row with the fields of r and the fields of r2.
// this might duplicate field names.
func (r Row) Concat(r2 Row) Row {
	ret := append(Row{}, r...)
	ret = append(ret, r2...)
	return ret
}

// Drop returns a new row without the fields identified by names
func (r Row) Drop(names ...string) Row {
	ret := Row{}
	for _, f := range r {
		if sliceIndex(names, f.Name) != -1 {
			continue
		}
		ret = append(ret, F(f.Name, f.Value))
	}
	return ret
}

// Prefix returns a new row with all fields prefixed with prefix.
func (r Row) Prefix(prefix string) Row {
	ret := Row{}
	for _, f := range r {
		ret = append(ret, F(prefix+f.Name, f.Value))
	}
	return ret
}

// Index returns the index of a field by name.
func (r Row) Index(s string) int {
	for i, c := range r {
		if c.Name == s {
			return i
		}
	}
	return -1
}

// WithField returns a copy of the row with the field f.
func (r Row) WithField(s string, v any) Row {
	nr := r.clone()
	for i, c := range nr {
		if c.Name == s {
			r[i].Value = v
			return r
		}
	}
	return append(nr, F(s, v))
}

// WithFields returns a copy of the row with the several fields
// if the field name exists the value will be replaced in the new row.
func (r Row) WithFields(fs ...Field) Row {
	nr := r.clone()
	for _, f := range fs {
		nr.set(f.Name, f.Value)
	}
	return nr
}

// Select returns a new row with the named selected fields.
func (r Row) Select(cols ...IntOrString) Row {
	newRow := Row{}
	for _, c := range cols {
		// Exceptional case if type returns several rows we return it.
		if fn, ok := c.(func(Row) Row); ok {
			res := fn(r)
			if len(res) > 0 {
				newRow = append(newRow, res...)
			}
			continue
		}
		f := r.at(c)
		if f == nil {
			continue
		}
		newRow = append(newRow, *f)
	}
	return newRow
}

// Rename returns a new row with the renamed field.
func (r Row) Rename(old, name string) Row {
	ret := r.clone()
	for i, c := range ret {
		if c.Name == old {
			ret[i].Name = name
		}
	}
	return ret
}

// RenameFields renames multiple fields based on map
func (r Row) RenameFields(m map[string]string) Row {
	ret := r.clone()
	for i, c := range ret {
		if n, ok := m[c.Name]; ok {
			ret[i].Name = n
		}
	}
	return ret
}

// InfoTree shows the tree of the row with fields and sub fields.
func (r Row) InfoTree() string {
	buf := &bytes.Buffer{}
	fmt.Fprint(buf, "\n")
	var fn func(Row, int)
	fn = func(row Row, lvl int) {
		for _, f := range row {
			fmt.Fprintf(buf, "%s%s: %T\n", strings.Repeat("  ", lvl+1), f.Name, f.Value)
			if r, ok := f.Value.(Row); ok {
				fn(r, lvl+1)
			}
		}
	}
	fn(r, 0)
	return buf.String()
}

// Info returns a string with the fields and values type of the row.
func (r Row) Info() string {
	buf := &bytes.Buffer{}
	fmt.Fprint(buf, "\n")
	fmt.Fprintf(buf, "Row: %d fields\n", len(r))
	for _, f := range r {
		fmt.Fprintf(buf, "  %s: %T\n", f.Name, f.Value)
	}
	return buf.String()
}

// at returns the field by name or index, nil if non existent.
func (r Row) at(s IntOrString) *Field {
	switch s := s.(type) {
	case int:
		if s < 0 || s >= len(r) {
			return nil
		}
		return &r[s]
	case string:
		for _, f := range r {
			if f.Name == s {
				return &f
			}
		}
		return nil
	case func(Row) *Field:
		return s(r)
	case FieldExpr:
		return s.Apply(r)
	default:
		panic(fmt.Sprintf("invalid type for At: %T", s))
	}
}

// Hide this
func (r *Row) set(key string, val any) {
	i := r.Index(key)
	if i < 0 {
		*r = append(*r, F(key, val))
		return
	}
	(*r)[i].Value = val
}

// Index returns the index of the first occurrence of val in the slice.
func sliceIndex[T comparable](hay []T, needle T) int {
	for i := range hay {
		if hay[i] == needle {
			return i
		}
	}
	return -1
}
