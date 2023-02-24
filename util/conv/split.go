package conv

import (
	"strings"
)

// SplitTo splits a string using sep as a delimiter and assigns the parts to the
// out parameters.
func SplitTo(s string, sep string, out ...*string) {
	parts := strings.Split(s, sep)
	for i, p := range parts {
		if i >= len(out) {
			break
		}
		if out[i] == nil {
			continue
		}
		*out[i] = p
	}
}
