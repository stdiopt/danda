package conv

// ToAnySlice converts a []T to []any, useful to pass params.
func ToAnySlice[T any](vs []T) []any {
	ret := make([]any, len(vs))
	for i, v := range vs {
		ret[i] = v
	}
	return ret
}

// Convert []any to a typed T if an element has an invalid converstion a zero T
// will be used
func ToNumberSlice[T Numbers](vs []any) []T {
	var z T
	ret := make([]T, len(vs))
	for i, v := range vs {
		ret[i] = Conv(z, v)
	}
	return ret
}

// NumberSlice converts a slice of numbers (int8,int16, and so on ...)
func NumberSlice[T, S Numbers](vs []S) []T {
	ret := make([]T, len(vs))
	for i := range vs {
		ret[i] = T(vs[i])
	}
	return ret
}
