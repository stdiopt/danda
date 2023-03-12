package conv

import (
	"reflect"
)

func Deref(v any) any {
	if v == nil {
		return nil
	}
	// Why?, because v at each one of this cases is typed
	// and the deref generic function will handle the type
	switch v := v.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return v
	case *string:
		return deref(v)
	case *int:
		return deref(v)
	case *int8:
		return deref(v)
	case *int16:
		return deref(v)
	case *int32:
		return deref(v)
	case *int64:
		return deref(v)
	case *uint:
		return deref(v)
	case *uint8:
		return deref(v)
	case *uint16:
		return deref(v)
	case *uint32:
		return deref(v)
	case *uint64:
		return deref(v)
	case *float32:
		return deref(v)
	case *float64:
		return deref(v)
	default:
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Pointer {
			if val.IsNil() {
				return nil
			}
			return val.Elem().Interface()
		}
		return v
	}
}

func deref[T any](v *T) any {
	if v == nil {
		return nil
	}
	return *v
}
