package etlfs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/stdiopt/danda/drow"
	"github.com/stdiopt/danda/etl"
)

// Find returns an interator that yields all files in the directory tree that
// matches pattern.
func Find(path, pattern string) Iter {
	return etl.MakeGen(etl.Gen[string]{
		Run: func(yield etl.Y[string]) error {
			return findFiles(os.DirFS("."), path, pattern, yield)
		},
	})
}

// FindDrow returns an iterator that yeilds a row containing information of the
// files in path that matches pattern.
func FindAsDrow(path, pattern string) Iter {
	return etl.MakeGen(etl.Gen[Row]{
		Run: func(yield etl.Y[Row]) error {
			return findFiles(os.DirFS("."), path, pattern, func(p string) error {
				s, err := os.Stat(p)
				if err != nil {
					return fmt.Errorf("error stat: %q, %w", p, err)
				}
				return yield(Row{
					drow.F("path", p),
					drow.F("size", s.Size()),
					drow.F("mode", s.Mode()),
					drow.F("modtime", s.ModTime()),
					drow.F("isdir", s.IsDir()),
				})
			})
		},
	})
}

func findFiles(fsys fs.FS, path, pattern string, yield func(string) error) error {
	return fs.WalkDir(fsys, path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		matched, err := filepath.Match(pattern, filepath.Base(path))
		if err != nil {
			return err
		}
		if matched {
			if err := yield(path); err != nil {
				return err
			}
		}
		return nil
	})
}
