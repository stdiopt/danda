package drow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// UnmarshalJSON implements the json.Unmarshaler interface.
func (r *Row) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))

	v, err := jsonReadValue(dec, nil)
	if err != nil {
		return err
	}

	switch v := v.(type) {
	case Row:
		*r = v
	default:
		return fmt.Errorf("Row.UnmarshalJSON: unexpected type: %T", v)
	}

	return nil
}

func (r Row) MarshalJSON() ([]byte, error) {
	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "{")
	for i, f := range r {
		data, err := json.Marshal(f.Value)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(buf, "%q:", f.Name)
		buf.Write(data)
		if i < len(r)-1 {
			fmt.Fprintf(buf, ",")
		}
	}
	fmt.Fprintf(buf, "}")
	return buf.Bytes(), nil
}

func jsonReadArray(dec *json.Decoder) ([]any, error) {
	var data []any
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		if v, ok := tok.(json.Delim); ok && v == ']' {
			return data, nil
		}

		v, err := jsonReadValue(dec, tok)
		if err != nil {
			return nil, err
		}
		data = append(data, v)
	}
}

func jsonReadValue(dec *json.Decoder, tok json.Token) (any, error) {
	if tok == nil {
		t, err := dec.Token()
		if err != nil {
			return nil, err
		}
		tok = t
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			return jsonReadObject(dec)
		case '[':
			arr, err := jsonReadArray(dec)
			if err != nil {
				return arr, err
			}
			if len(arr) == 0 {
				return arr, nil
			}
			// Make it a single type, might be expensive
			typ := reflect.TypeOf(arr[0])
			for _, a := range arr {
				if typ != reflect.TypeOf(a) {
					return arr, nil
				}
			}
			slice := reflect.New(reflect.SliceOf(typ)).Elem()
			for _, a := range arr {
				slice = reflect.Append(slice, reflect.ValueOf(a))
			}

			return slice.Interface(), nil

		}
	default:
		return t, nil
	}
	return nil, errors.New("unexpected error?")
}

func jsonReadObject(dec *json.Decoder) (Row, error) {
	var row Row
	for {
		tok, err := dec.Token()
		if err != nil {
			return Row{}, err
		}
		if t, ok := tok.(json.Delim); ok && t == '}' {
			return row, nil
		}

		key, ok := tok.(string)
		if !ok {
			return Row{}, fmt.Errorf("unexpected: %q", key)
		}
		value, err := jsonReadValue(dec, nil)
		if err != nil {
			return Row{}, err
		}
		row.set(key, value)
	}
}
