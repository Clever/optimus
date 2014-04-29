package transform

import (
	"github.com/azylman/getl"
)

// A getl.Table that performs a given transformation on evern element in the input table.
type elTransformedTable struct {
	input     getl.Table
	transform func(getl.Row) (getl.Row, error)
	err       error
	row       getl.Row
}

func (t *elTransformedTable) Scan() bool {
	if t.input.Scan() == false {
		t.err = t.input.Err()
		return false
	}
	row, err := t.transform(t.input.Row())
	if err != nil {
		t.err = err
		return false
	}
	t.row = row
	return true
}

func (t elTransformedTable) Row() getl.Row {
	return t.row
}

func (t elTransformedTable) Err() error {
	return t.err
}

// A transformer is a helper struct for chaining transformations on a table.
type transformer struct {
	table getl.Table
}

func (t transformer) Table() getl.Table {
	return t.table
}

// Constructs an elTransformedTable from an input table and a transform function.
func elTransform(table getl.Table, transform func(getl.Row) (getl.Row, error)) getl.Table {
	return &elTransformedTable{
		input:     table,
		transform: transform,
	}

}

// Fieldmap returns a Table that has all the rows of the input Table with the field mapping applied.
func Fieldmap(table getl.Table, mappings map[string][]string) getl.Table {
	return elTransform(table, func(row getl.Row) (getl.Row, error) {
		newRow := getl.Row{}
		for key, vals := range mappings {
			for _, val := range vals {
				newRow[val] = row[key]
			}
		}
		return newRow, nil
	})
}

func (t *transformer) Fieldmap(mappings map[string][]string) *transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = Fieldmap(t.table, mappings)
	return t
}

// NewTransformer returns a Transformer that allows you to chain transformations on a table.
func NewTransformer(table getl.Table) *transformer {
	return &transformer{table}
}
