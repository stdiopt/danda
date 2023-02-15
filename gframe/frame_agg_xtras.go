package gframe

import "github.com/stdiopt/danda/util/conv"

// GroupByFunc wrap a function that takes a row and returns a row to be used as
// the group key.
func GroupByFunc(fn func(Row) Row) aggGroupFn {
	return GroupByFuncE(func(v Row) (Row, error) {
		return fn(v), nil
	})
}

// GroupBy returns a group function that uses the selected fields on a row to
// be used as the group key.
func GroupBy(s ...IntOrString) aggGroupFn {
	return GroupByFunc(func(row Row) Row {
		return row.Select(s...)
	})
}

// AggCount returns an aggregation option that counts the field identified by name.
func AggCount(name string, field ...string) aggOptFn {
	as := asName(name, field...)
	return Reduce(as, func(acc int, row Row) int {
		if v := row.At(name).Value; v != nil {
			return acc + 1
		}
		return acc
	})
}

// AggMax returns an aggregation option that returns the maximum value of a field.
func AggMax(name string, field ...string) aggOptFn {
	as := asName(name, field...)
	return Reduce(as, func(acc float64, row Row) float64 {
		if v := row.At(name).Float64(); v > acc {
			return v
		}
		return acc
	})
}

// AggSum returns an aggregation option that returns the sum of a field.
func AggSum(name string, field ...string) aggOptFn {
	as := asName(name, field...)
	return Reduce(as, func(acc float64, row Row) float64 {
		return acc + row.At(name).Float64()
	})
}

// AggMean returns an aggregation option that returns the mean value of a field.
func AggMean(name string, field ...string) aggOptFn {
	as := asName(name, field...)
	type meanData struct {
		count int
		sum   float64
	}
	return Reduce(as,
		func(acc meanData, row Row) meanData {
			return meanData{
				count: acc.count + 1,
				sum:   acc.sum + conv.Conv[float64](0, row.At(name)),
			}
		},
		func(acc meanData) any {
			return acc.sum / float64(acc.count)
		},
	)
}

// AggFirst returns an aggregation option that returns the first occurrence of the group.
func AggFirst(name string, field ...string) aggOptFn {
	as := asName(name, field...)
	return Reduce(as, func(acc any, row Row) any {
		if acc == nil {
			return row.At(name)
		}
		return acc
	})
}

func asName(name string, field ...string) string {
	if len(field) > 0 {
		return field[0]
	}
	return name
}
