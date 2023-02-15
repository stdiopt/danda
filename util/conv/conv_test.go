package conv

import (
	"fmt"
	"reflect"
	"testing"
)

func convtest[T Numbers]() func(any) any {
	return func(v any) any {
		return Conv[T](0, v)
	}
}

func TestConv(t *testing.T) {
	type args struct {
		v any
	}

	type test struct {
		args args
		fn   func(any) any
		want any
	}

	run := func(name string, tt test) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			t.Helper()
			if got := tt.fn(tt.args.v); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("conv() = %v, want %v", got, tt.want)
			}
		})
	}

	numbers := []any{
		int(73),
		int8(73),
		int16(73),
		int32(73),
		int64(73),
		uint(73),
		uint8(73),
		uint16(73),
		uint32(73),
		uint64(73),
		float32(73),
		float64(73),
		"73",
		[]byte("73"),
	}
	for _, n := range numbers {
		run(fmt.Sprintf("%T_to_int", n), test{args{v: n}, convtest[int](), 73})
		run(fmt.Sprintf("%T_to_int8", n), test{args{v: n}, convtest[int8](), int8(73)})
		run(fmt.Sprintf("%T_to_int16", n), test{args{v: n}, convtest[int16](), int16(73)})
		run(fmt.Sprintf("%T_to_int32", n), test{args{v: n}, convtest[int32](), int32(73)})
		run(fmt.Sprintf("%T_to_int64", n), test{args{v: n}, convtest[int64](), int64(73)})
		run(fmt.Sprintf("%T_to_uint", n), test{args{v: n}, convtest[uint](), uint(73)})
		run(fmt.Sprintf("%T_to_uint8", n), test{args{v: n}, convtest[uint8](), uint8(73)})
		run(fmt.Sprintf("%T_to_uint16", n), test{args{v: n}, convtest[uint16](), uint16(73)})
		run(fmt.Sprintf("%T_to_uint32", n), test{args{v: n}, convtest[uint32](), uint32(73)})
		run(fmt.Sprintf("%T_to_uint64", n), test{args{v: n}, convtest[uint64](), uint64(73)})
		run(fmt.Sprintf("%T_to_float32", n), test{args{v: n}, convtest[float32](), float32(73)})
		run(fmt.Sprintf("%T_to_float64", n), test{args{v: n}, convtest[float64](), float64(73)})
	}
	// Is this endian dependant?
	run("allow overflow", test{args{v: uint32(0xFFFF1010)}, convtest[uint8](), uint8(0x10)})
	run("floatstring_to_float32", test{args{v: "73.57"}, convtest[float64](), float64(73.57)})
	run("floatstring_to_float64", test{args{v: "73.57"}, convtest[float64](), float64(73.57)})
}
