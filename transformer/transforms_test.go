package transformer

import (
	"errors"
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/infinite"
	"github.com/azylman/getl/sources/slice"
	"testing"
)

var errorTransform = func(msg string) func(getl.Row) (getl.Row, error) {
	return func(getl.Row) (getl.Row, error) {
		return nil, errors.New(msg)
	}
}

var transformEqualities = []tableCompareConfig{
	{
		name: "Fieldmap",
		actual: func(getl.Table, interface{}) getl.Table {
			return Fieldmap(defaultSource(), map[string][]string{"header1": {"header4"}})
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
		name: "RowTransform",
		actual: func(getl.Table, interface{}) getl.Table {
			return RowTransform(defaultSource(), func(row getl.Row) (getl.Row, error) {
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
		name: "TableTransform",
		actual: func(getl.Table, interface{}) getl.Table {
			return TableTransform(defaultSource(), func(row getl.Row, out chan<- getl.Row) error {
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
	{
		name: "SelectEverything",
		actual: func(getl.Table, interface{}) getl.Table {
			return Select(defaultSource(), func(row getl.Row) (bool, error) {
				return true, nil
			})
		},
		expected: func(getl.Table, interface{}) getl.Table {
			return defaultSource()
		},
	},
	{
		name: "SelectNothing",
		actual: func(getl.Table, interface{}) getl.Table {
			return Select(defaultSource(), func(row getl.Row) (bool, error) {
				return false, nil
			})
		},
		expected: func(getl.Table, interface{}) getl.Table {
			return slice.New([]getl.Row{})
		},
	},
	{
		name: "Valuemap",
		actual: func(getl.Table, interface{}) getl.Table {
			mapping := map[string]map[interface{}]interface{}{
				"header1": {"value1": "value10", "value3": "value30"},
			}
			return Valuemap(defaultSource(), mapping)
		},
		expected: func(getl.Table, interface{}) getl.Table {
			return slice.New([]getl.Row{
				{"header1": "value10", "header2": "value2"},
				{"header1": "value30", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
			})
		},
	},
	{
		name: "TableTransformErrorPassesThrough",
		actual: func(getl.Table, interface{}) getl.Table {
			return New(infinite.New()).RowTransform(
				errorTransform("failed")).Fieldmap(map[string][]string{}).Table()
		},
		error: errors.New("failed"),
	},
	{
		name: "TableTransformFirstErrorPassesThrough",
		actual: func(getl.Table, interface{}) getl.Table {
			return New(infinite.New()).RowTransform(
				errorTransform("failed1")).RowTransform(errorTransform("failed2")).Table()
		},
		error: errors.New("failed1"),
	},
}

func TestTransforms(t *testing.T) {
	compareTables(t, transformEqualities)
}
