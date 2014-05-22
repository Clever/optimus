package transformer

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/slice"
	"testing"
)

var transformEqualities = []tableCompareConfig{
	{
		name:   "Fieldmap",
		source: defaultSource,
		actual: func(source getl.Table, _ interface{}) getl.Table {
			return Fieldmap(source, map[string][]string{"header1": {"header4"}})
		},
		expected: func(getl.Table, interface{}) getl.Table {
			return slice.New([]getl.Row{
				{"header4": "value1"},
				{"header4": "value3"},
				{"header4": "value5"},
			})
		},
	},
	{
		name:   "RowTransform",
		source: defaultSource,
		actual: func(source getl.Table, _ interface{}) getl.Table {
			return RowTransform(source, func(row getl.Row) (getl.Row, error) {
				row["troll_key"] = "troll_value"
				return row, nil
			})
		},
		expected: func(getl.Table, interface{}) getl.Table {
			rows := defaultInput()
			for _, row := range rows {
				row["troll_key"] = "troll_value"
			}
			return slice.New(rows)
		},
	},
	{
		name:   "TableTransform",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			return TableTransform(source, func(row getl.Row, out chan getl.Row) error {
				out <- row
				out <- getl.Row{}
				return nil
			})
		},
		expected: func(getl.Table, interface{}) getl.Table {
			rows := defaultInput()
			newRows := []getl.Row{}
			for _, row := range rows {
				newRows = append(newRows, row)
				newRows = append(newRows, getl.Row{})
			}
			return slice.New(newRows)
		},
	},
}

func TestTransforms(t *testing.T) {
	compareTables(t, transformEqualities)
}
