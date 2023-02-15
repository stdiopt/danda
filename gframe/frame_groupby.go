package gframe

// {lpf} This is mostly an alias of Agg funcs.

// FrameGroupBy contains data to perform a groupby operation.
type FrameGroupBy struct {
	frame Frame
	grpFn aggGroupFn
	aggs  []aggOptFn
	err   error
}

// Frame apply transformations and return new frame.
func (g FrameGroupBy) Frame() Frame {
	if g.err != nil {
		return ErrFrame(g.err)
	}
	return g.frame.Group(g.grpFn, g.aggs...)
}

func (g FrameGroupBy) with(aggOpts ...aggOptFn) FrameGroupBy {
	aggs := append([]aggOptFn{}, g.aggs...)
	return FrameGroupBy{
		frame: g.frame,
		grpFn: g.grpFn,
		aggs:  append(aggs, aggOpts...),
		err:   g.err,
	}
}

// Count counts the number of occurrences of the specific field.
func (g FrameGroupBy) Count(field string, as ...string) FrameGroupBy {
	return g.with(AggCount(field, as...))
}

// Max returns the biggest value in the specific field.
func (g FrameGroupBy) Max(field string, as ...string) FrameGroupBy {
	return g.with(AggMax(field, as...))
}

// Sum sums the values of the specific field.
func (g FrameGroupBy) Sum(field string, as ...string) FrameGroupBy {
	return g.with(AggSum(field, as...))
}

// First returns the first value of the specific field in the group.
func (g FrameGroupBy) First(field string, as ...string) FrameGroupBy {
	return g.with(AggFirst(field, as...))
}

// Mean returns the avg value of the specific field.
func (g FrameGroupBy) Mean(field string, as ...string) FrameGroupBy {
	return g.with(AggMean(field, as...))
}

// Custom adds a custom reduce function to the groupby operation.
func (g FrameGroupBy) Custom(opts ...aggOptFn) FrameGroupBy {
	return g.with(opts...)
}

// Err returns the error of the groupby operation if any.
func (g FrameGroupBy) Err() error {
	return g.err
}

// GroupBy initiates a groupby operation.
func (f Frame) GroupBy(cols ...IntOrString) FrameGroupBy {
	return FrameGroupBy{
		frame: f,
		grpFn: GroupByFunc(func(row Row) Row {
			return row.Select(cols...)
		}),
	}
}

// GroupByFunc initiates a groupby operation by accepting a function that will return
// a row where all fields are the key.
func (f Frame) GroupByFunc(fn func(row Row) Row) FrameGroupBy {
	return FrameGroupBy{
		frame: f,
		grpFn: GroupByFunc(func(row Row) Row {
			return fn(row)
		}),
	}
}

// GroupByFuncE initiates a groupby operation by accepting a function that will return
// a row where all fields are the key and an error if any.
func (f Frame) GroupByFuncE(fn func(row Row) (Row, error)) FrameGroupBy {
	return FrameGroupBy{
		frame: f,
		grpFn: GroupByFuncE(fn),
	}
}
