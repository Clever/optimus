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

var chainedEqualities = []tableCompareConfig{
	{
		name:   "Fieldmap",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			mappings := arg.(map[string][]string)
			return New(source).Fieldmap(mappings).Table()
		},
		expected: func(source getl.Table, arg interface{}) getl.Table {
			mappings := arg.(map[string][]string)
			return Fieldmap(source, mappings)
		},
		arg: map[string][]string{"header1": {"header4"}},
	},
	{
		name:   "RowTransform",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return New(source).RowTransform(transform).Table()
		},
		expected: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return RowTransform(source, transform)
		},
		arg: func(row getl.Row) (getl.Row, error) {
			return getl.Row{}, nil
		},
	},
	{
		name:   "Table",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row, chan<- getl.Row) error)
			return New(source).TableTransform(transform).Table()
		},
		expected: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row, chan<- getl.Row) error)
			return TableTransform(source, transform)
		},
		arg: func(row getl.Row, out chan<- getl.Row) error {
			out <- getl.Row{}
			out <- getl.Row{}
			out <- getl.Row{}
			return nil
		},
	},
	{
		name:   "Select",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			filter := arg.(func(getl.Row) (bool, error))
			return New(source).Select(filter).Table()
		},
		expected: func(source getl.Table, arg interface{}) getl.Table {
			filter := arg.(func(getl.Row) (bool, error))
			return Select(source, filter)
		},
		arg: func(row getl.Row) (bool, error) {
			return row["header1"] == "value1", nil
		},
	},
	{
		name:   "Valuemap",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			mapping := arg.(map[string]map[interface{}]interface{})
			return New(source).Valuemap(mapping).Table()
		},
		expected: func(source getl.Table, arg interface{}) getl.Table {
			mapping := arg.(map[string]map[interface{}]interface{})
			return Valuemap(source, mapping)
		},
		arg: map[string]map[interface{}]interface{}{
			"header1": {"value1": "value10", "value3": "value30"},
		},
	},
}

// TestEquality tests that the chained version and non-chained version of a transform
// have the same result, given the same input and options.
func TestEquality(t *testing.T) {
	compareTables(t, chainedEqualities)
}
