package transformer

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/slice"
	"github.com/azylman/getl/tests"
	"github.com/stretchr/testify/assert"
	"testing"
)

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
			return Fieldmap(source, mappings)
		},
		Arg: map[string][]string{"header1": {"header4"}},
	},
	{
		Name:   "RowTransform",
		Source: defaultSource,
		Actual: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return New(source).RowTransform(transform).Table()
		},
		Expected: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return RowTransform(source, transform)
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
			return TableTransform(source, transform)
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
			return Select(source, filter)
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
			return Valuemap(source, mapping)
		},
		Arg: map[string]map[interface{}]interface{}{
			"header1": {"value1": "value10", "value3": "value30"},
		},
	},
}

// TestEquality tests that the chained version and non-chained version of a transform
// have the same result, given the same input and options.
func TestEquality(t *testing.T) {
	tests.CompareTables(t, chainedEqualities)
}
