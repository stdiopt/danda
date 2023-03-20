package gframe

import (
	"sort"
	"time"
)

func (f Frame) OrderBy() FrameOrderBy {
	return FrameOrderBy{frame: f}
}

func (f Frame) Order(lessfn func(r1, r2 Row) bool) Frame {
	indexes := make([]int, f.Len())
	for i := range indexes {
		indexes[i] = i
	}
	s := &gsort{df: f, lessfn: lessfn, indexes: indexes}
	sort.Sort(s)

	b := rowFrameBuilder{}

	for _, i := range s.indexes {
		if err := b.Add(f.Row(i)); err != nil {
			return ErrFrame(err)
		}
	}
	return b.Frame()
}

type fieldComp struct {
	name IntOrString
	less func(v1, v2 any) bool
}

type FrameOrderBy struct {
	frame     Frame
	fieldComp []fieldComp
}

func (f FrameOrderBy) Asc(fields ...IntOrString) FrameOrderBy {
	fc := append([]fieldComp{}, f.fieldComp...)
	for _, field := range fields {
		fc = append(fc, fieldComp{
			name: field,
			less: less,
		})
	}
	return FrameOrderBy{frame: f.frame, fieldComp: fc}
}

func (f FrameOrderBy) Desc(fields ...IntOrString) FrameOrderBy {
	fc := append([]fieldComp{}, f.fieldComp...)
	for _, field := range fields {
		fc = append(fc, fieldComp{
			name: field,
			less: func(v1, v2 any) bool {
				return less(v2, v1)
			},
		})
	}
	return FrameOrderBy{frame: f.frame, fieldComp: fc}
}

// Farme apply order and return new frame
func (f FrameOrderBy) Frame() Frame {
	return f.frame.Order(func(r1, r2 Row) bool {
		for _, fc := range f.fieldComp {
			v1 := r1.At(fc.name).Value
			v2 := r2.At(fc.name).Value
			if v1 == v2 {
				continue
			}
			return fc.less(v1, v2)
		}
		return false
	})
}

// gsoert sorts the indexes of a frame
type gsort struct {
	df      Frame
	lessfn  func(r1, r2 Row) bool
	indexes []int
}

// Len is the number of elements in the collection.
func (g *gsort) Len() int {
	return g.df.Len()
}

func (g *gsort) Less(i int, j int) bool {
	i, j = g.indexes[i], g.indexes[j]
	return g.lessfn(g.df.Row(i), g.df.Row(j))
}

// Swap swaps the elements with indexes i and j.
func (g *gsort) Swap(i int, j int) {
	g.indexes[i], g.indexes[j] = g.indexes[j], g.indexes[i]
}

// One of those..
func less(v1, v2 any) bool {
	switch v1 := v1.(type) {
	case int:
		return v1 < v2.(int)
	case int8:
		return v1 < v2.(int8)
	case int16:
		return v1 < v2.(int16)
	case int32:
		return v1 < v2.(int32)
	case int64:
		return v1 < v2.(int64)
	case uint:
		return v1 < v2.(uint)
	case uint8:
		return v1 < v2.(uint8)
	case uint16:
		return v1 < v2.(uint16)
	case uint32:
		return v1 < v2.(uint32)
	case uint64:
		return v1 < v2.(uint64)
	case float32:
		return v1 < v2.(float32)
	case float64:
		return v1 < v2.(float64)
	case string:
		return v1 < v2.(string)
	case bool:
		return !v1 && v2.(bool)
	case time.Time:
		return v1.Before(v2.(time.Time))
	default:
		panic("not supported type")
	}
}
