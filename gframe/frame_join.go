package gframe

type joinOnFunc = func(r1, r2 Row) bool

// LeftJoin performs a left join on two dataframes.
func (f Frame) LeftJoin(df Frame, fn joinOnFunc) Frame { return sideJoin(f, df, false, fn) }

// RightJoin performs a right join on two dataframes.
func (f Frame) RightJoin(df Frame, fn joinOnFunc) Frame { return sideJoin(f, df, true, fn) }

// InnerJoin performs a inner join on two dataframes.
func (f Frame) InnerJoin(df Frame, fn joinOnFunc) Frame { return innerJoin(f, df, fn) }

// OuterJoin performs a outer join on two dataframes.
func (f Frame) OuterJoin(df Frame, fn joinOnFunc) Frame { return outerJoin(f, df, fn) }

// JoinOn is a convinient function to create a joinOnFunc.
func JoinOn(f1, f2 string) func(r1, r2 Row) bool {
	return func(r1, r2 Row) bool {
		return r1.At(f1).Value == r2.At(f2).Value
	}
}

// Only return existing in both frames.
func innerJoin(df1, df2 Frame, fn joinOnFunc) Frame {
	rows := []Row{}
	err := df1.Each(func(r Row) error {
		return df2.Each(func(r2 Row) error {
			if !fn(r, r2) {
				return nil
			}
			rows = append(rows, r.Concat(r2))
			return nil
		})
	})
	if err != nil {
		return ErrFrame(err)
	}
	return FromRows(rows)
}

func outerJoin(df1, df2 Frame, fn joinOnFunc) Frame {
	rows := []Row{}
	// Create a zero right row
	zr1 := Row{}
	for _, f := range df1.Row(0) {
		zr1 = zr1.WithField(f.Name, nil)
	}
	zr2 := Row{}
	for _, f := range df2.Row(0) {
		zr2 = zr2.WithField(f.Name, nil)
	}

	added := map[int]struct{}{}
	err := df1.Each(func(r Row) error {
		match := false
		err := df2.EachI(func(r2i int, r2 Row) error {
			if !fn(r, r2) {
				return nil
			}
			added[r2i] = struct{}{}

			match = true
			rows = append(rows, r.Concat(r2))
			return nil
		})
		if err != nil {
			return err
		}
		if !match {
			rows = append(rows, r.Concat(zr2))
		}
		return nil
	})
	if err != nil {
		return ErrFrame(err)
	}
	// Add the rest of not matched rows
	err = df2.EachI(func(i int, r Row) error {
		_, ok := added[i]
		if !ok {
			rows = append(rows, zr1.Concat(r))
		}
		return nil
	})
	if err != nil {
		return ErrFrame(err)
	}
	return FromRows(rows)
}

// If c is true a,b will be returned as b,a
func condInv[T any](c bool, a, b T) (T, T) {
	if c {
		return b, a
	}
	return a, b
}

func sideJoin(df1, df2 Frame, right bool, fn joinOnFunc) Frame {
	rows := []Row{}
	a, b := df1, df2

	if right {
		b, a = a, b
	}
	// Create a zero right row
	zr := Row{}
	for _, f := range b.Row(0) {
		zr = zr.WithField(f.Name, nil)
	}
	err := a.Each(func(r Row) error {
		match := false
		err := b.Each(func(r2 Row) error {
			c1, c2 := condInv(right, r, r2)
			if !fn(c1, c2) {
				return nil
			}
			match = true
			rows = append(rows, c1.Concat(c2))

			return nil
		})
		if err != nil {
			return err
		}
		if !match {
			c1, c2 := condInv(right, r, zr)
			rows = append(rows, c1.Concat(c2))
		}
		return nil
	})
	if err != nil {
		return ErrFrame(err)
	}
	return FromRows(rows)
}
