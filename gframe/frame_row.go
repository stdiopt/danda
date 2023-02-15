package gframe

// rowFrameBuilder builds a DFrame based on Rows by merging fields by name.
type rowFrameBuilder struct {
	cols   []string
	chunks map[string]*seriesChunk[any]
	count  int
}

// Add adds a row.
func (rs *rowFrameBuilder) Add(r Row) error {
	if rs.chunks == nil {
		rs.chunks = make(map[string]*seriesChunk[any])
	}
	ri := rs.count
	// Add a nil to every series
	for _, s := range rs.chunks {
		s.data = append(s.data, nil)
	}
	rs.count++
	// if the series does not exists we create a new one else we just set
	// on the added index from above
	for _, f := range r {
		chunk, ok := rs.chunks[f.Name]
		if !ok {
			chunk = &seriesChunk[any]{
				offset: ri,
				data:   []any{f.Value},
			}
			rs.cols = append(rs.cols, f.Name)
			rs.chunks[f.Name] = chunk
			continue
		}
		chunk.data[len(chunk.data)-1] = f.Value
	}
	return nil
}

// Series returns a slice of series based on the added rows.
func (rs *rowFrameBuilder) Series() []Series {
	series := make([]Series, len(rs.cols))
	for i, c := range rs.cols {
		chunk := rs.chunks[c]
		series[i] = Series{name: c}.WithValues(chunk.offset, chunk.data...)
	}
	return series
}

// Frame returns a DFrame based on the added rows.
func (rs *rowFrameBuilder) Frame() Frame {
	return Frame{series: rs.Series()}
}
