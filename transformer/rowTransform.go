package transformer

import (
	"github.com/azylman/getl"
)

// RowTransform returns a Table that applies a transform function to every row in the input table.
func RowTransform(input getl.Table, transform func(getl.Row) (getl.Row, error)) getl.Table {
	return TableTransform(input, func(in getl.Row, out chan getl.Row) error {
		row, err := transform(in)
		if err != nil {
			return err
		}
		out <- row
		return nil
	})
}
