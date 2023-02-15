// Package humanize provides functions to format numbers with units.
package humanize

// could be in specific package etlhumanize maybe? too big?
// or just humanize

import (
	"fmt"
)

type num64 interface {
	int | uint64 | int64 | float64
}

// Number formats a number with a unit.
func Number[T num64](b T) string {
	return humanize(b, "kMBTQ", "", 1000)
}

// Bytes formats a number of bytes with a unit.
func Bytes[T num64](b T) string {
	return humanize(b, "kMGTPE", "B", 1024)
}

func humanize[T num64](b T, units, suffix string, unit int64) string {
	if b < T(unit) {
		return fmt.Sprintf("%.0f %s", float64(b), suffix)
	}
	div, exp := unit, 0
	for n := b / T(unit); n >= T(unit) && exp < len(units); n /= T(unit) {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %c%s", float64(b)/float64(div), units[exp], suffix)
}
