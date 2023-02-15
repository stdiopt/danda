package gframe

import (
	"reflect"
	"testing"
)

func TestSeries_Len(t *testing.T) {
	type test struct {
		series func() Series
		want   int
	}

	run := func(name string, tt test) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			t.Helper()
			s := tt.series()
			if got := s.Len(); got != tt.want {
				t.Errorf("Series.Len()\nwant: %v\n got: %v", tt.want, got)
			}
		})
	}

	run("returns 0 if empty", test{
		series: func() Series {
			return S[int]("test")
		},
		want: 0,
	})

	run("returns data length", test{
		series: func() Series {
			return S("test", 1, 2)
		},
		want: 2,
	})
	run("returns provider source len", test{
		series: func() Series {
			s := S[int]("test")
			return s.WithValues(0, 1, 2, 3)
		},
		want: 3,
	})
	run("returns data first len even if provider is set", test{
		series: func() Series {
			s := S("test", 1, 2, 3)
			s = s.Clone().WithValues(0, 1, 2)
			return s
		},
		want: 3,
	})
}

func TestSeries_Get(t *testing.T) {
	type test struct {
		series func() Series
		count  int
		want   []any
	}
	run := func(name string, tt test) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			t.Helper()
			s := tt.series()
			count := tt.count
			if count <= 0 {
				count = s.Len()
			}
			got := []any{}
			for i := 0; i < count; i++ {
				got = append(got, s.At(i))
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Get()\nwant: %q\n got: %q", tt.want, got)
			}
		})
	}

	run("returns nil on empty Series", test{
		series: func() Series {
			return Series{}
		},
		count: 1,
		want:  []any{nil},
	})
	run("returns zero value on empty data", test{
		series: func() Series {
			return SP("test", &SeriesData[int]{})
		},
		count: 1,
		want:  []any{0},
	})
	run("returns value from provider", test{
		series: func() Series {
			return S("test", 1, 2)
		},
		count: 2,
		want:  []any{1, 2},
	})
	run("returns value from provide with 0 out of boundr", test{
		series: func() Series {
			return S("test", 1, 2)
		},
		count: 3,
		want:  []any{1, 2, 0},
	})
	run("returns value from cloned", test{
		series: func() Series {
			return S("test", 1, 2).Clone()
		},
		count: 2,
		want:  []any{1, 2},
	})
	run("returns value from data and source", test{
		series: func() Series {
			return S("test", 1, 2).WithValues(0, 3)
		},
		count: 2,
		want:  []any{3, 2},
	})
}
