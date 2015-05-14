package csv

import (
	"io"

	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/mgo.v2/bson"
)

// New writes all of the Rows in a Table as newline-separate JSON objects.
func New(out io.Writer) optimus.Sink {
	return func(source optimus.Table) error {
		defer source.Stop()
		for row := range source.Rows() {
			obj, err := bson.Marshal(row)
			if err != nil {
				return err
			}
			obj = append(obj, byte('\n'))
			if _, err := out.Write(obj); err != nil {
				return err
			}
		}
		if source.Err() != nil {
			return source.Err()
		}
		return nil
	}
}
