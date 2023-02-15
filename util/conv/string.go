package conv

import (
	"fmt"
	"reflect"
)

func ToString(v any) string {
	if v == nil {
		return ""
	}
	switch v := v.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		val := reflect.ValueOf(v)
		if val.Kind() == reflect.Ptr && val.IsNil() {
			return ""
		}
		// We might need to reflect to detect pointers
		return fmt.Sprint(val)
	}
}
