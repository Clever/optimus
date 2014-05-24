package transformer

import (
	"errors"
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/infinite"
	"github.com/azylman/getl/sources/slice"
	"github.com/azylman/getl/tests"
	"github.com/azylman/getl/transforms"
	"github.com/stretchr/testify/assert"
	"testing"
)

var defaultInput = func() []getl.Row {
	return []getl.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
	}
}

var defaultSource = func() getl.Table {
	return slice.New(defaultInput())
}

var errorTransform = func(msg string) func(getl.Row) (getl.Row, error) {
	return func(getl.Row) (getl.Row, error) {
		return nil, errors.New(msg)
	}
}

// Test that chaining together multiple transforms behaves as expected
func TestChaining(t *testing.T) {
	input := []getl.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
	}
	expected := []getl.Row{
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
		Actual: func(source getl.Table, arg interface{}) getl.Table {
			mappings := arg.(map[string][]string)
			return New(source).Fieldmap(mappings).Table()
		},
		Expected: func(source getl.Table, arg interface{}) getl.Table {
			mappings := arg.(map[string][]string)
			return getl.Transform(source, transforms.Fieldmap(mappings))
		},
		Arg: map[string][]string{"header1": {"header4"}},
	},
	{
		Name:   "Map",
		Source: defaultSource,
		Actual: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return New(source).Map(transform).Table()
		},
		Expected: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return getl.Transform(source, transforms.Map(transform))
		},
		Arg: func(row getl.Row) (getl.Row, error) {
			return getl.Row{}, nil
		},
	},
	{
		Name:   "Table",
		Source: defaultSource,
		Actual: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row, chan<- getl.Row) error)
			return New(source).TableTransform(transform).Table()
		},
		Expected: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row, chan<- getl.Row) error)
			return getl.Transform(source, transforms.TableTransform(transform))
		},
		Arg: func(row getl.Row, out chan<- getl.Row) error {
			out <- getl.Row{}
			out <- getl.Row{}
			out <- getl.Row{}
			return nil
		},
	},
	{
		Name:   "Select",
		Source: defaultSource,
		Actual: func(source getl.Table, arg interface{}) getl.Table {
			filter := arg.(func(getl.Row) (bool, error))
			return New(source).Select(filter).Table()
		},
		Expected: func(source getl.Table, arg interface{}) getl.Table {
			filter := arg.(func(getl.Row) (bool, error))
			return getl.Transform(source, transforms.Select(filter))
		},
		Arg: func(row getl.Row) (bool, error) {
			return row["header1"] == "value1", nil
		},
	},
	{
		Name:   "Valuemap",
		Source: defaultSource,
		Actual: func(source getl.Table, arg interface{}) getl.Table {
			mapping := arg.(map[string]map[interface{}]interface{})
			return New(source).Valuemap(mapping).Table()
		},
		Expected: func(source getl.Table, arg interface{}) getl.Table {
			mapping := arg.(map[string]map[interface{}]interface{})
			return getl.Transform(source, transforms.Valuemap(mapping))
		},
		Arg: map[string]map[interface{}]interface{}{
			"header1": {"value1": "value10", "value3": "value30"},
		},
	},
	{
		Name: "TableTransformErrorPassesThrough",
		Actual: func(getl.Table, interface{}) getl.Table {
			return New(infinite.New()).Map(
				errorTransform("failed")).Fieldmap(map[string][]string{}).Table()
		},
		Error: errors.New("failed"),
	},
	{
		Name: "TableTransformFirstErrorPassesThrough",
		Actual: func(getl.Table, interface{}) getl.Table {
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
