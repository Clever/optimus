package transforms

import (
	"github.com/azylman/getl"
)

// TableTransform returns a Table that has applies the given transform function to the output channel.
func TableTransform(transform func(getl.Row, chan<- getl.Row) error) getl.TransformFunc {
	return func(in <-chan getl.Row, out chan<- getl.Row) error {
		for row := range in {
			if err := transform(row, out); err != nil {
				return err
			}
		}
		return nil
	}
}

// Select returns a Table that only has Rows that pass the filter.
func Select(filter func(getl.Row) (bool, error)) getl.TransformFunc {
	return TableTransform(func(row getl.Row, out chan<- getl.Row) error {
		pass, err := filter(row)
		if err != nil || !pass {
			return err
		}
		out <- row
		return nil
	})
}

// Map returns a Table that applies a transform function to every row in the input table.
func Map(transform func(getl.Row) (getl.Row, error)) getl.TransformFunc {
	return TableTransform(func(in getl.Row, out chan<- getl.Row) error {
		row, err := transform(in)
		if err != nil {
			return err
		}
		out <- row
		return nil
	})
}

// Each returns a Table that passes through all the Rows from the source table, invoking a function
// for each.
func Each(fn func(getl.Row) error) {
	return Map(func(row getl.Row) (getl.Row, error) {
		if err := fn(row); err != nil {
			return err
		}
		return row
	})
}

// Fieldmap returns a Table that has all the Rows of the input Table with the field mapping applied.
func Fieldmap(mappings map[string][]string) getl.TransformFunc {
	return Map(func(row getl.Row) (getl.Row, error) {
		newRow := getl.Row{}
		for key, vals := range mappings {
			for _, val := range vals {
				newRow[val] = row[key]
			}
		}
		return newRow, nil
	})
}

// Valuemap returns a Table that has all the Rows of the input Table with a value mapping applied.
func Valuemap(mappings map[string]map[interface{}]interface{}) getl.TransformFunc {
	return Map(func(row getl.Row) (getl.Row, error) {
		newRow := getl.Row{}
		for key, val := range row {
			if mappings[key] == nil || mappings[key][val] == nil {
				newRow[key] = val
				continue
			}
			newRow[key] = mappings[key][val]
		}
		return newRow, nil
	})
}
