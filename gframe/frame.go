package gframe

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/stdiopt/danda/etl"
	"github.com/stdiopt/danda/util/conv"
)

type iterator interface {
	Next(context.Context) (any, error)
	Close() error
}

// From make a new dataframe from a list of series
func From(data any) Frame {
	switch v := data.(type) {
	case Frame:
		return v.clone()
	case []Series:
		return Frame{series: append([]Series{}, v...)}
	case []Row:
		return FromRows(v)
	case Row:
		return FromRows([]Row{v})
	case iterator:
		return FromIter(v)
	// case []map[string]any:
	//	return FrameFromMaps(data)
	default:
		panic(fmt.Sprintf("From(): %T unsupported", data))
	}
}

// FromRows creates a frame from a slice of rows.
func FromRows(rows []Row) Frame {
	if len(rows) == 0 {
		return Frame{}
	}
	rs := rowFrameBuilder{}
	for _, r := range rows {
		if err := rs.Add(r); err != nil {
			return ErrFrame(err)
		}
	}
	return Frame{series: rs.Series()}
}

// FromIter loads an iterator into a data frame.
func FromIter(it iterator) Frame {
	defer it.Close()
	rs := rowFrameBuilder{}
	for {
		var row Row
		vv, err := it.Next(context.TODO())
		if err == etl.EOI {
			break
		}
		if err != nil {
			return ErrFrame(err)
		}

		switch v := vv.(type) {
		case Row:
			row = v
		default:
			row = Row{Field{Name: "_unnamed", Value: v}}
			// OR Create a new raw Row
			// return ErrFrame(fmt.Errorf("FromIter: value type unexpected: %T", vv))
		}
		if row == nil { // ignore nil rows?!
			continue
		}

		if err := rs.Add(row); err != nil {
			return ErrFrame(err)
		}
	}

	return Frame{series: rs.Series()}
}

// ErrFrame returns a dataframe with an error.
func ErrFrame(err error) Frame {
	return Frame{err: err}
}

// New creates a new dataframe with the given series
func New(series ...Series) Frame {
	return Frame{series: series}.clone()
}

// Frame contains data series.
type Frame struct {
	series []Series
	err    error
}

// Err returns the dataframe error if any
func (f Frame) Err() error {
	return f.err
}

// Iter returns a dataframe iterator
func (f Frame) Iter() etl.Iter {
	return &IterFrame{df: &f}
}

// Series returns the series identified by n of the dataframe.
func (f Frame) Series(n string) Series {
	for _, s := range f.series {
		if s.name == n {
			return s
		}
	}
	return Series{}
}

// Columns returns the series names.
func (f Frame) Columns() []string {
	ret := make([]string, len(f.series))
	for i, s := range f.series {
		ret[i] = s.Name()
	}
	return ret
}

// Row retrieves a row from the dataframe.
func (f Frame) Row(i int) Row {
	row := make([]Field, len(f.series))
	for fi, s := range f.series {
		row[fi] = Field{Name: s.Name(), Value: s.At(i)}
	}
	return row
}

// Rows returns all rows of the dataframe.
func (f Frame) Rows() []Row {
	rows := make([]Row, f.Len())
	for i := range rows {
		rows[i] = f.Row(i)
	}
	return rows
}

func (f Frame) At(k IntOrString, i int) Field {
	return f.Row(i).At(k)
}

// Insert return a new dataframe with the inserted series.
func (f Frame) Insert(sers ...Series) Frame {
	nf := f.clone()
	nf.series = append(nf.series, sers...)
	return nf
}

// Drop returns a new dataframe without named serie
func (f Frame) Drop(names ...string) Frame {
	nf := f.clone()
	for _, n := range names {
		for i, s := range nf.series {
			if s.name == n {
				nf.series = append(nf.series[:i], nf.series[i+1:]...)
				break
			}
		}
	}
	return nf
}

// DropRows returns a new data frame with specific rows removed.
func (f Frame) DropRows(indexes ...int) Frame {
	nf := f.clone()
	for si := range nf.series {
		nf.series[si] = nf.series[si].Remove(indexes...)
	}
	return nf
}

// Slice all series in the dataframe to the given range
func (f Frame) Slice(start, end int) Frame {
	nf := f.clone()
	sz := end - start
	for i, s := range nf.series {
		nf.series[i] = s.Slice(start, sz)
	}
	return nf
}

// Head returns the first n rows of the dataframe as a new dataframe
// default is 5
func (f Frame) Head(n ...int) Frame {
	if len(n) == 0 {
		return f.Slice(0, 5)
	}
	return f.Slice(0, n[0])
}

// Select returns a new dataframe with the selected series
func (f Frame) Select(fields ...IntOrString) Frame {
	// Hard copy, without the soft stuff
	rows := []Row{}
	for ri := 0; ri < f.Len(); ri++ {
		rows = append(rows, f.Row(ri).Select(fields...))
	}
	return FromRows(rows)
}

// Rename renames a series in the dataframe
func (f Frame) Rename(o, n string) Frame {
	nf := f.clone()
	for i, s := range nf.series {
		if s.name == o {
			nf.series[i] = s.WithName(n)
		}
	}
	return nf
}

// Prefix prefixes all series names with p.
func (f Frame) Prefix(p string) Frame {
	nf := f.clone()
	for i, s := range nf.series {
		nf.series[i] = s.WithName(p + s.name)
	}
	return nf
}

// Map iterates over the dataframe and applies the fn to each row
func (f Frame) Map(fn func(Row) Row) Frame {
	var series []Series
	err := f.Each(func(row Row) error {
		if row == nil { // ignore nil rows
			return nil
		}
		row = fn(row)
		// Produce several series type based on row consumption
		if series == nil {
			series = make([]Series, len(row))
			for i := range series {
				series[i] = Series{name: row[i].Name}
			}
		}
		for i, f := range row {
			series[i] = series[i].Append(f.Value)
		}
		return nil
	})
	if err != nil {
		return ErrFrame(err)
	}
	return Frame{series: series}
}

type FilterFunc func(Row) bool

// Filter iterates over the dataframe if the fn returns true it will forward
// the row to the next dataframe
func (f Frame) Filter(fn FilterFunc) Frame {
	var series []Series
	err := f.Each(func(row Row) error {
		if row == nil { // ignore nil rows!?
			return nil
		}
		if !fn(row) {
			return nil
		}
		// Produce several series type based on row consumption
		if series == nil {
			series = make([]Series, len(row))
			for i := range series {
				series[i] = Series{name: row[i].Name}
			}
		}
		for i, f := range row {
			series[i] = series[i].Append(f.Value)
		}
		return nil
	})
	if err != nil {
		return ErrFrame(err)
	}
	return Frame{series: series}
}

// Each calls fn for each row in the dataframe.
func (f Frame) Each(fn func(Row) error) error {
	for i := 0; i < f.Len(); i++ {
		if err := fn(f.Row(i)); err != nil {
			return err
		}
	}
	return nil
}

// EachI calls fn with each row and index in the dataframe.
func (f Frame) EachI(fn func(int, Row) error) error {
	for i := 0; i < f.Len(); i++ {
		if err := fn(i, f.Row(i)); err != nil {
			return err
		}
	}
	return nil
}

// Len returns the number of rows in the dataframe
// i.e: the biggest series in the dataframe
func (f Frame) Len() int {
	m := 0
	for _, s := range f.series {
		m = max(s.Len(), m)
	}
	return m
}

// Print prints a markdown formated dataframe using the regular fmt.Println
func (f Frame) Print() {
	fmt.Println(f)
}

// String will render a markdown compatible table into a string
func (f Frame) String() string {
	if f.err != nil {
		return fmt.Sprintf("ErrFrame: %s", f.err)
	}
	buf := &bytes.Buffer{}

	l := f.Len() // Max dimensions (shape)

	colLen := make([]int, len(f.series))

	// Check max text size by checking header then data
	for ci := 0; ci < len(f.series); ci++ { // columns
		s := f.series[ci]
		colLen[ci] = max(colLen[ci], len(s.Name()))
		colLen[ci] = max(colLen[ci], len(fmt.Sprintf("%T", s.At(0))))
	}
	for ri := 0; ri < l; ri++ { // rows
		for ci := 0; ci < len(f.series); ci++ { // columns
			val := fmt.Sprint(f.series[ci].At(ri))
			colLen[ci] = max(colLen[ci], len(val))
		}
	}

	// render Header
	for i, s := range f.series {
		if i != 0 {
			fmt.Fprint(buf, " | ")
		}
		fmt.Fprintf(buf, " %-*s", colLen[i], s.Name())
	}
	fmt.Fprintf(buf, "\n")

	// render Header types
	for i, s := range f.series {
		if i != 0 {
			fmt.Fprint(buf, " | ")
		}
		fmt.Fprintf(buf, " %-*T", colLen[i], s.At(0))
	}
	fmt.Fprintf(buf, "\n")

	// Render line
	for i := 0; i < len(f.series); i++ {
		if i != 0 {
			fmt.Fprint(buf, "-|-")
		}
		fmt.Fprintf(buf, "-%s", strings.Repeat("-", colLen[i]))
	}
	fmt.Fprintf(buf, "\n")

	// Draw values
	for ri := 0; ri < l; ri++ {
		for ci := 0; ci < len(f.series); ci++ {
			if ci != 0 {
				fmt.Fprint(buf, " | ")
			}
			val := fmt.Sprint(conv.Deref(f.series[ci].At(ri)))
			fmt.Fprintf(buf, " %-*v", colLen[ci], val)
		}
		fmt.Fprintf(buf, "\n")
	}

	fmt.Fprintf(buf, "number of rows: %d", l)

	return buf.String()
}

func (f Frame) clone() Frame {
	series := append([]Series{}, f.series...)
	return Frame{series: series, err: f.err}
}

// New implementations, to be moved to a proper place

func (f Frame) seriesAt(s string) int {
	for i, series := range f.series {
		if series.name == s {
			return i
		}
	}
	return -1
}

// AppendRows appends rows to the frame by mapping the row fields into the proper series
func (f Frame) AppendRows(rows ...Row) Frame {
	nd := f.clone()
	for _, row := range rows {
		l := nd.Len()
		for _, ff := range row {
			si := nd.seriesAt(ff.Name)
			if si == -1 {
				s := Series{name: ff.Name}.WithValues(l, ff.Value)
				nd.series = append(nd.series, s)
				continue
			}
			nd.series[si] = nd.series[si].Append(ff.Value)
		}
	}
	return nd
}

func (f Frame) Info() string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "\nFrame: %d series, %d rows\n", len(f.series), f.Len())
	for _, s := range f.series {
		fmt.Fprintf(buf, "  %s: %T\n", s.Name(), s.At(0))
	}
	return buf.String()
}
