package csv

import (
	"encoding/json"
	"gopkg.in/azylman/optimus.v1"
	"os"
)

// New writes all of the Rows in a Table as newline-separate JSON objects.
func New(filename string) optimus.Sink {
	return func(source optimus.Table) error {
		fout, err := os.Create(filename)
		defer fout.Close()
		if err != nil {
			return err
		}

		for row := range source.Rows() {
			obj, err := json.Marshal(row)
			if err != nil {
				return err
			}
			obj = append(obj, byte('\n'))
			if _, err := fout.Write(obj); err != nil {
				return err
			}
		}
		if source.Err() != nil {
			return source.Err()
		}
		if err := fout.Sync(); err != nil {
			return err
		}
		return nil
	}
}
