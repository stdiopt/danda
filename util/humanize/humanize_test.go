package humanize_test

import (
	"testing"

	"github.com/stdiopt/danda/util/humanize"
)

func TestHumanizeB(t *testing.T) {
	type args struct {
		b uint64
	}
	type test struct {
		args args
		want string
	}

	run := func(name string, tt test) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			t.Helper()
			if got := humanize.Bytes(tt.args.b); got != tt.want {
				t.Errorf("HumanizeB() = %v, want %v", got, tt.want)
			}
		})
	}

	run("byte", test{args: args{b: 1}, want: "1 B"})
	run("kilo", test{args: args{b: 1 << 10}, want: "1.0 kB"})
	run("mega", test{args: args{b: 1 << 20}, want: "1.0 MB"})
	run("giga", test{args: args{b: 1 << 30}, want: "1.0 GB"})
	run("tera", test{args: args{b: 1 << 40}, want: "1.0 TB"})
	run("peta", test{args: args{b: 1 << 50}, want: "1.0 PB"})
	run("exa", test{args: args{b: 1 << 60}, want: "1.0 EB"})
}
