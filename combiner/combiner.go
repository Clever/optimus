package combiner

import (
	"github.com/azylman/optimus"
	"github.com/azylman/optimus/sources/slice"
)

// A Combiner allows you to join two tables
type Combiner struct {
	leftTable  optimus.Table
	rightTable optimus.Table
}

// joinRows is an intermediate structure used by the hash-join
type joinRows struct {
	leftRows  []optimus.Row
	rightRows []optimus.Row
}

// Join merges two tables based on shared column values for the specified headers
func (c Combiner) Join(leftHeader string, rightHeader string) optimus.Table {
	// TODO: Allow join on multiple headers, i.e. `matchHeaders map[string]string`
	//  which describes which leftTable headers map to which rightTable headers
	hash := make(map[interface{}]joinRows)

	// Setup hash from left table
	for row := range c.leftTable.Rows() {
		if _, ok := hash[row[leftHeader]]; ok {
			// Allow non-unique values for join index in left table
			var temp = hash[row[leftHeader]]
			temp.leftRows = append(temp.leftRows, row)
			hash[row[leftHeader]] = temp
		} else {
			hash[row[leftHeader]] = joinRows{[]optimus.Row{row}, nil}
		}
	}

	// Join right table
	for row := range c.rightTable.Rows() {
		if _, ok := hash[row[rightHeader]]; ok {
			var temp = hash[row[rightHeader]]
			temp.rightRows = append(temp.rightRows, row)
			hash[row[rightHeader]] = temp
		}
	}

	// Build joined table
	outputRows := []optimus.Row{}
	for _, joinRows := range hash {
		for _, leftRow := range joinRows.leftRows {
			for _, rightRow := range joinRows.rightRows {
				newRow := mergeRows(leftRow, rightRow)
				outputRows = append(outputRows, newRow)
			}
		}
	}
	return slice.New(outputRows)
}

// Extend combines two tables of length x and y into one table of length (x+y)
func (c Combiner) Extend() optimus.Table {
	allRows := []optimus.Row{}

	for row := range c.leftTable.Rows() {
		allRows = append(allRows, row)
	}

	for row := range c.rightTable.Rows() {
		allRows = append(allRows, row)
	}

	return slice.New(allRows)
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

// New returns a Combiner that allows you to join tables.
func New(leftTable optimus.Table, rightTable optimus.Table) *Combiner {
	return &Combiner{leftTable, rightTable}
}
