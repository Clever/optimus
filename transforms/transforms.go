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

type joinStruct struct {
	Left, Inner joinType
}

type joinType struct {
	int
}

// Left: Always add row from Left table, even if no corresponding rows found in Right table)
// Inner: Only add row from Left table if corresponding row(s) found in Right table)
var JoinType = joinStruct{Left: joinType{0}, Inner: joinType{1}}

// Join returns a Table that combines fields with another table, joining via joinType
func Join(rightTable optimus.Table, leftHeader string, rightHeader string, join joinType) optimus.TransformFunc {
	hash := make(map[interface{}][]optimus.Row)

	// Build hash from right table
	done := make(chan struct{})
	go func() {
		defer close(done)
		for row := range rightTable.Rows() {
			// Initialize if dne
			if val := hash[row[rightHeader]]; val == nil {
				hash[row[rightHeader]] = []optimus.Row{}
			}
			hash[row[rightHeader]] = append(hash[row[rightHeader]], row)
		}
	}()

	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		<-done
		if rightTable.Err() != nil {
			return rightTable.Err()
		}

		for leftRow := range in {
			// if value is in the hash
			if rightRows := hash[leftRow[leftHeader]]; rightRows != nil {
				// for each row for that hash value
				for _, rightRow := range rightRows {
					// join and send it to the out channel
					out <- mergeRows(leftRow, rightRow)
				}
			} else {
				if join == JoinType.Left {
					out <- leftRow
				}
			}
		}
		return nil
	}
}

func mergeRows(src optimus.Row, dst optimus.Row) optimus.Row {
	output := optimus.Row{}
	for k, v := range src {
		output[k] = v
	}
	for k, v := range dst {
		output[k] = v
	}
	return output
}

// Reduce returns a Table that has all the rows reduced into a single row.
func Reduce(fn func(accum, item optimus.Row) error) optimus.TransformFunc {
	return func(in <-chan optimus.Row, out chan<- optimus.Row) error {
		accum := optimus.Row{}
		for row := range in {
			if err := fn(accum, row); err != nil {
				return err
			}
		}
		out <- accum
		return nil
	}
}
