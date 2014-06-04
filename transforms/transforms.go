package transforms

import (
	"github.com/azylman/optimus"
)

// TableTransform returns a Table that has applies the given transform function to the output channel.
func TableTransform(transform func(optimus.Row, chan<- optimus.Row) error) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		for row := range in {
			if err := transform(row, out); err != nil {
				return err
			}
		}
		return nil
	}
}

// Select returns a Table that only has Rows that pass the filter.
func Select(filter func(optimus.Row) (bool, error)) optimus.TransformFunc {
	return TableTransform(func(row optimus.Row, out chan<- optimus.Row) error {
		pass, err := filter(row)
		if err != nil || !pass {
			return err
		}
		out <- row
		return nil
	})
}

// Map returns a Table that returns the results of calling the transform function for every row.
func Map(transform func(optimus.Row) (optimus.Row, error)) optimus.TransformFunc {
	return TableTransform(func(in optimus.Row, out chan<- optimus.Row) error {
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
func Each(fn func(optimus.Row) error) optimus.TransformFunc {
	return Map(func(row optimus.Row) (optimus.Row, error) {
		if err := fn(row); err != nil {
			return nil, err
		}
		return row, nil
	})
}

// Fieldmap returns a Table that has all the Rows of the input Table with the field mapping applied.
func Fieldmap(mappings map[string][]string) optimus.TransformFunc {
	return Map(func(row optimus.Row) (optimus.Row, error) {
		newRow := optimus.Row{}
		for key, vals := range mappings {
			for _, val := range vals {
				newRow[val] = row[key]
			}
		}
		return newRow, nil
	})
}

// Valuemap returns a Table that has all the Rows of the input Table with a value mapping applied.
func Valuemap(mappings map[string]map[interface{}]interface{}) optimus.TransformFunc {
	return Map(func(row optimus.Row) (optimus.Row, error) {
		newRow := optimus.Row{}
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
