package etltmpl

import (
	"fmt"
	"html/template"
	"strings"

	"github.com/stdiopt/danda/etl"
)

type Iter = etl.Iter

type options struct {
	Funcs template.FuncMap
}

type OptFunc func(*options)

func WithFuncs(fns template.FuncMap) OptFunc {
	return func(o *options) {
		if o.Funcs == nil {
			o.Funcs = make(template.FuncMap)
		}
		for k, v := range fns {
			o.Funcs[k] = v
		}
	}
}

func makeOptions(opts ...OptFunc) options {
	o := options{
		Funcs: template.FuncMap{
			"fgcolor": termFColor,
			"bgcolor": termBColor,
			"color":   termColor,
		},
	}
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// Template processes all values in iterator through template and sends result
// to output iterator.
func Template(it Iter, t string, opts ...OptFunc) Iter {
	o := makeOptions(opts...)
	tmpl, err := template.New("iter.Template").Funcs(o.Funcs).Parse(t)
	if err != nil {
		return etl.ErrIter(err)
	}
	return etl.MakeIter(etl.Custom[string]{
		Next: func() (string, error) {
			v, err := it.Next()
			if err != nil {
				return "", err
			}

			var b strings.Builder
			if err := tmpl.Execute(&b, v); err != nil {
				return "", err
			}
			return b.String(), nil
		},
		Close: it.Close,
	})
}

func termFColor(color uint32, v any) any {
	r, g, b := byte(color>>16), byte(color>>8), byte(color)
	return fmt.Sprintf("\033[38;2;%d;%d;%dm%v\033[0m", r, g, b, v)
}

func termBColor(color uint32, v any) any {
	r, g, b := byte(color>>16), byte(color>>8), byte(color)
	return fmt.Sprintf("\033[48;2;%d;%d;%dm%v\033[0m", r, g, b, v)
}

func termColor(fg, bg uint32, v any) any {
	return fmt.Sprintf("\033[38;2;%d;%d;%dm\033[48;2;%d;%d;%dm%v\033[0m",
		byte(fg>>16), byte(fg>>8), byte(fg),
		byte(bg>>16), byte(bg>>8), byte(bg),
		v,
	)
}
