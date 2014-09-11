package transformer

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/infinite"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
	"gopkg.in/Clever/optimus.v3/transforms"
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

var errorTransform = func(msg string) func(optimus.Row) (optimus.Row, error) {
	return func(optimus.Row) (optimus.Row, error) {
		return nil, errors.New(msg)
	}
}

// Test that chaining together multiple transforms behaves as expected
func TestChaining(t *testing.T) {
	input := []optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
	}
	expected := []optimus.Row{
		{"header4": "value1"},
		{"header4": "value3"},
		{"header4": "value5"},
	}

	table := slice.New(input)
	transformedTable := New(table).Fieldmap(
		map[string][]string{"header1": {"header3"}}).Fieldmap(map[string][]string{"header3": {"header4"}}).Table()
	rows := tests.HasRows(t, transformedTable, 3)
	assert.Equal(t, expected, rows)
}

var chainedEqualities = []tests.TableCompareConfig{
	{
		Name:   "Fieldmap",
		Source: defaultSource,
		Actual: func(source optimus.Table, arg interface{}) optimus.Table {
			mappings := arg.(map[string][]string)
			return New(source).Fieldmap(mappings).Table()
		},
		Expected: func(source optimus.Table, arg interface{}) optimus.Table {
			mappings := arg.(map[string][]string)
			return optimus.Transform(source, transforms.Fieldmap(mappings))
		},
		Arg: map[string][]string{"header1": {"header4"}},
	},
	{
		Name:   "Map",
		Source: defaultSource,
		Actual: func(source optimus.Table, arg interface{}) optimus.Table {
			transform := arg.(func(optimus.Row) (optimus.Row, error))
			return New(source).Map(transform).Table()
		},
		Expected: func(source optimus.Table, arg interface{}) optimus.Table {
			transform := arg.(func(optimus.Row) (optimus.Row, error))
			return optimus.Transform(source, transforms.Map(transform))
		},
		Arg: func(row optimus.Row) (optimus.Row, error) {
			return optimus.Row{}, nil
		},
	},
	{
		Name:   "Table",
		Source: defaultSource,
		Actual: func(source optimus.Table, arg interface{}) optimus.Table {
			transform := arg.(func(optimus.Row, chan<- optimus.Row) error)
			return New(source).TableTransform(transform).Table()
		},
		Expected: func(source optimus.Table, arg interface{}) optimus.Table {
			transform := arg.(func(optimus.Row, chan<- optimus.Row) error)
			return optimus.Transform(source, transforms.TableTransform(transform))
		},
		Arg: func(row optimus.Row, out chan<- optimus.Row) error {
			out <- optimus.Row{}
			out <- optimus.Row{}
			out <- optimus.Row{}
			return nil
		},
	},
	{
		Name:   "Select",
		Source: defaultSource,
		Actual: func(source optimus.Table, arg interface{}) optimus.Table {
			filter := arg.(func(optimus.Row) (bool, error))
			return New(source).Select(filter).Table()
		},
		Expected: func(source optimus.Table, arg interface{}) optimus.Table {
			filter := arg.(func(optimus.Row) (bool, error))
			return optimus.Transform(source, transforms.Select(filter))
		},
		Arg: func(row optimus.Row) (bool, error) {
			return row["header1"] == "value1", nil
		},
	},
	{
		Name:   "Valuemap",
		Source: defaultSource,
		Actual: func(source optimus.Table, arg interface{}) optimus.Table {
			mapping := arg.(map[string]map[interface{}]interface{})
			return New(source).Valuemap(mapping).Table()
		},
		Expected: func(source optimus.Table, arg interface{}) optimus.Table {
			mapping := arg.(map[string]map[interface{}]interface{})
			return optimus.Transform(source, transforms.Valuemap(mapping))
		},
		Arg: map[string]map[interface{}]interface{}{
			"header1": {"value1": "value10", "value3": "value30"},
		},
	},
	{
		Name: "TableTransformErrorPassesThrough",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return New(infinite.New()).Map(
				errorTransform("failed")).Fieldmap(map[string][]string{}).Table()
		},
		Error: errors.New("failed"),
	},
	{
		Name: "TableTransformFirstErrorPassesThrough",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return New(infinite.New()).Map(
				errorTransform("failed1")).Map(errorTransform("failed2")).Table()
		},
		Error: errors.New("failed1"),
	},
}

// TestEquality tests that the chained version and non-chained version of a transform
// have the same result, given the same input and options.
func TestEquality(t *testing.T) {
	tests.CompareTables(t, chainedEqualities)
}
