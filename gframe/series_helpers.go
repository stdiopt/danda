package gframe

import (
	"github.com/stdiopt/danda/util/conv"
)

func Sum[T conv.Numbers](s Series) T {
	var sum T
	for i := 0; i < s.Len(); i++ {
		sum += conv.Conv[T](0, s.At(i))
	}
	return sum
}

func Max[T conv.Numbers](s Series) T {
	if s.Len() == 0 {
		return 0
	}

	max := conv.Conv[T](0, s.At(0))
	for i := 1; i < s.Len(); i++ {
		v := conv.Conv[T](0, s.At(i))
		if max < v {
			max = v
		}
	}
	return max
}

func Min[T conv.Numbers](s Series) T {
	if s.Len() == 0 {
		return 0
	}

	min := conv.Conv[T](0, s.At(0))
	for i := 1; i < s.Len(); i++ {
		v := conv.Conv[T](0, s.At(i))
		if min > v {
			min = v
		}
	}
	return min
}
