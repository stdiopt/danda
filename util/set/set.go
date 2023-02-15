// Package set implements a set data structure.
package set

type comparator interface {
	Eq(any) bool
}

// Set is a set data structure of type T
type Set[T any] struct {
	Data []T
}

// Index returns the index of the value, -1 if not found.
func (m *Set[T]) Index(va T) int {
	for i, v := range m.Data {
		switch v := any(v).(type) {
		case comparator:
			if v.Eq(va) {
				return i
			}
		default:
			if v == any(va) {
				return i
			}
		}
	}
	return -1
}

// IndexOrAdd returns the existing or the new index of the value, it returns
// true if the value existed, false if new.
func (m *Set[T]) IndexOrAdd(va T) (int, bool) {
	ri := m.Index(va)
	if ri == -1 {
		return m.add(va), false
	}
	return ri, true
}

func (m *Set[T]) add(va T) int {
	ri := len(m.Data)
	m.Data = append(m.Data, va)
	return ri
}
