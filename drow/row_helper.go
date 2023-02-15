package drow

type FieldExpr struct {
	fn func(r Row) *Field
	as string
}

func (f FieldExpr) Apply(r Row) *Field {
	v := f.fn(r)
	if v == nil {
		return nil
	}
	name := f.as
	if name == "" {
		name = v.Name
	}
	return &Field{Name: name, Value: v.Value}
}

func (f FieldExpr) As(s string) FieldExpr {
	return FieldExpr{fn: f.fn, as: s}
}

// Computed row field
func FE(s ...IntOrString) FieldExpr {
	return FieldExpr{
		fn: func(r Row) *Field {
			return solve(r, s...)
		},
	}
}

func solve(r Row, s ...IntOrString) *Field {
	if len(s) == 0 {
		return nil
	}
	if len(s) == 1 {
		return r.at(s[0])
	}

	v := r.at(s[0])
	if v == nil {
		return nil
	}
	if sr, ok := v.Value.(Row); ok {
		vv := sr.at(FE(s[1:]...))
		if vv == nil {
			return nil
		}
		return &Field{v.Name + "/" + vv.Name, vv.Value}
	}

	return &Field{v.Name, v.Value}
}
