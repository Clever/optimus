package transformer

import (
	"errors"
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/infinite"
	"github.com/azylman/getl/sources/slice"
	"github.com/azylman/getl/tests"
	"testing"
)

var errorTransform = func(msg string) func(getl.Row) (getl.Row, error) {
	return func(getl.Row) (getl.Row, error) {
		return nil, errors.New(msg)
	}
}

var transformEqualities = []tests.TableCompareConfig{
	{
		Name: "Fieldmap",
		Actual: func(getl.Table, interface{}) getl.Table {
			return Fieldmap(defaultSource(), map[string][]string{"header1": {"header4"}})
		},
		Expected: func(getl.Table, interface{}) getl.Table {
			return slice.New([]getl.Row{
				{"header4": "value1"},
				{"header4": "value3"},
				{"header4": "value5"},
			})
		},
	},
	{
		Name: "RowTransform",
		Actual: func(getl.Table, interface{}) getl.Table {
			return RowTransform(defaultSource(), func(row getl.Row) (getl.Row, error) {
				row["troll_key"] = "troll_value"
				return row, nil
			})
		},
		Expected: func(getl.Table, interface{}) getl.Table {
			rows := defaultInput()
			for _, row := range rows {
				row["troll_key"] = "troll_value"
			}
			return slice.New(rows)
		},
	},
	{
		Name: "TableTransform",
		Actual: func(getl.Table, interface{}) getl.Table {
			return TableTransform(defaultSource(), func(row getl.Row, out chan<- getl.Row) error {
				out <- row
				out <- getl.Row{}
				return nil
			})
		},
		Expected: func(getl.Table, interface{}) getl.Table {
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
		Name: "SelectEverything",
		Actual: func(getl.Table, interface{}) getl.Table {
			return Select(defaultSource(), func(row getl.Row) (bool, error) {
				return true, nil
			})
		},
		Expected: func(getl.Table, interface{}) getl.Table {
			return defaultSource()
		},
	},
	{
		Name: "SelectNothing",
		Actual: func(getl.Table, interface{}) getl.Table {
			return Select(defaultSource(), func(row getl.Row) (bool, error) {
				return false, nil
			})
		},
		Expected: func(getl.Table, interface{}) getl.Table {
			return slice.New([]getl.Row{})
		},
	},
	{
		Name: "Valuemap",
		Actual: func(getl.Table, interface{}) getl.Table {
			mapping := map[string]map[interface{}]interface{}{
				"header1": {"value1": "value10", "value3": "value30"},
			}
			return Valuemap(defaultSource(), mapping)
		},
		Expected: func(getl.Table, interface{}) getl.Table {
			return slice.New([]getl.Row{
				{"header1": "value10", "header2": "value2"},
				{"header1": "value30", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
			})
		},
	},
	{
		Name: "TableTransformErrorPassesThrough",
		Actual: func(getl.Table, interface{}) getl.Table {
			return New(infinite.New()).RowTransform(
				errorTransform("failed")).Fieldmap(map[string][]string{}).Table()
		},
		Error: errors.New("failed"),
	},
	{
		Name: "TableTransformFirstErrorPassesThrough",
		Actual: func(getl.Table, interface{}) getl.Table {
			return New(infinite.New()).RowTransform(
				errorTransform("failed1")).RowTransform(errorTransform("failed2")).Table()
		},
		Error: errors.New("failed1"),
	},
}

func TestTransforms(t *testing.T) {
	tests.CompareTables(t, transformEqualities)
}

// TestTransformError tests that the upstream Table had all of its data consumed in the case of an
// error from a TableTransform.
func TestTransformError(t *testing.T) {
	in := infinite.New()
	out := TableTransform(in, func(row getl.Row, out chan<- getl.Row) error {
		return errors.New("some error")
	})
	// Should receive no rows here because the first response was an error.
	tests.Consumed(t, out)
	// Should receive no rows here because the the transform should have consumed
	// all the rows.
	tests.Consumed(t, in)
}
