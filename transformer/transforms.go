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

// Fieldmap returns a Table that has all the Rows of the input Table with the field mapping applied.
func Fieldmap(table getl.Table, mappings map[string][]string) getl.Table {
	return RowTransform(table, func(row getl.Row) (getl.Row, error) {
		newRow := getl.Row{}
		for key, vals := range mappings {
			for _, val := range vals {
				newRow[val] = row[key]
			}
		}
		return newRow, nil
	})
}
