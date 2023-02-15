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
