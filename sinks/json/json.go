package csv

import (
	"encoding/json"
	"github.com/azylman/optimus"
	"os"
)

// New writes all of the Rows in a Table as newline-separate JSON objects.
func New(source optimus.Table, filename string) error {
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
