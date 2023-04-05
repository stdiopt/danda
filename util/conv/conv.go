// Package conv provides a simple way to convert between types.
package conv

import (
	"reflect"
	"strconv"
	"strings"

	"golang.org/x/exp/constraints"
)

type Numbers interface {
	constraints.Integer | constraints.Float
}

// Conv this can lose data i.e converting int32 to int8
func Conv[T Numbers](def T, v any) T {
	var z T
	switch v := v.(type) {
	case nil:
		return def
	// Convert to string first, then parse
	// might not be ideal since we could read a number from binary?
	case []byte:
		return Conv(def, string(v))
	case string:
		v = strings.TrimSpace(v)
		switch any(z).(type) {
		case uint, uint8, uint16, uint32, uint64:
			r, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return z
			}
			return T(r)
		case int, int8, int16, int32, int64:
			r, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return z
			}
			return T(r)
		case float32, float64:
			r, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return z
			}
			return T(r)
		}
		return z
	case int8:
		return T(v)
	case int16:
		return T(v)
	case int32:
		return T(v)
	case int64:
		return T(v)
	case int:
		return T(v)
	case float32:
		return T(v)
	case float64:
		return T(v)
	case uint:
		return T(v)
	case uint8:
		return T(v)
	case uint16:
		return T(v)
	case uint32:
		return T(v)
	case uint64:
		return T(v)
	// TODO: add more pointer to native types
	case *int8:
		if v == nil {
			return def
		}
		return T(*v)
	default: // returns default on no conversion possible
		// Extra case in case if it is a pointer we dereference it
		// and try again
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Ptr {
			if val.IsZero() {
				return def
			}
			val = val.Elem()
			return Conv(def, val.Interface())
		}
		return def
	}
}
