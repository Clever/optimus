package transforms

import (
	"errors"
	"github.com/azylman/optimus"
	"github.com/azylman/optimus/sources/infinite"
	"github.com/azylman/optimus/sources/slice"
	"github.com/azylman/optimus/tests"
	"testing"
)

var defaultInput = func() []optimus.Row {
	return []optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
	}
}

var defaultSource = func() optimus.Table {
	return slice.New(defaultInput())
}

var transformEqualities = []tests.TableCompareConfig{
	{
		Name: "Fieldmap",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Fieldmap(map[string][]string{"header1": {"header4"}}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header4": "value1"},
				{"header4": "value3"},
				{"header4": "value5"},
			})
		},
	},
	{
		Name: "Map",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Map(func(row optimus.Row) (optimus.Row, error) {
				row["troll_key"] = "troll_value"
				return row, nil
			}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			rows := defaultInput()
			for _, row := range rows {
				row["troll_key"] = "troll_value"
			}
			return slice.New(rows)
		},
	},
	{
		Name: "TableTransform",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), TableTransform(func(row optimus.Row, out chan<- optimus.Row) error {
				out <- row
				out <- optimus.Row{}
				return nil
			}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			rows := defaultInput()
			newRows := []optimus.Row{}
			for _, row := range rows {
				newRows = append(newRows, row)
				newRows = append(newRows, optimus.Row{})
			}
			return slice.New(newRows)
		},
	},
	{
		Name: "SelectEverything",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Select(func(row optimus.Row) (bool, error) {
				return true, nil
			}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return defaultSource()
		},
	},
	{
		Name: "SelectNothing",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Select(func(row optimus.Row) (bool, error) {
				return false, nil
			}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{})
		},
	},
	{
		Name: "Valuemap",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			mapping := map[string]map[interface{}]interface{}{
				"header1": {"value1": "value10", "value3": "value30"},
			}
			return optimus.Transform(defaultSource(), Valuemap(mapping))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header1": "value10", "header2": "value2"},
				{"header1": "value30", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
			})
		},
	},
}

func TestTransforms(t *testing.T) {
	tests.CompareTables(t, transformEqualities)
}

// TestTransformError tests that the upstream Table had all of its data consumed in the case of an
// error from a TableTransform.
func TestTransformError(t *testing.T) {
	in := infinite.New()
	out := optimus.Transform(in, TableTransform(func(row optimus.Row, out chan<- optimus.Row) error {
		return errors.New("some error")
	}))
	// Should receive no rows here because the first response was an error.
	tests.Consumed(t, out)
	// Should receive no rows here because the the transform should have consumed
	// all the rows.
	tests.Consumed(t, in)
}
