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

type equalityConfig struct {
	name      string
	source    func() getl.Table
	chained   func(getl.Table, interface{}) getl.Table
	unchained func(getl.Table, interface{}) getl.Table
	arg       interface{}
}

var defaultSource = func() getl.Table {
	return slice.New([]getl.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
	})
}

var transforms = []equalityConfig{
	{
		name:   "Fieldmap",
		source: defaultSource,
		chained: func(source getl.Table, arg interface{}) getl.Table {
			mappings := arg.(map[string][]string)
			return New(source).Fieldmap(mappings).Table()
		},
		unchained: func(source getl.Table, arg interface{}) getl.Table {
			mappings := arg.(map[string][]string)
			return Fieldmap(source, mappings)
		},
		arg: map[string][]string{"header1": {"header4"}},
	},
	{
		name:   "RowTransform",
		source: defaultSource,
		chained: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return New(source).RowTransform(transform).Table()
		},
		unchained: func(source getl.Table, arg interface{}) getl.Table {
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
		chained: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row, chan getl.Row) error)
			return New(source).TableTransform(transform).Table()
		},
		unchained: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row, chan getl.Row) error)
			return TableTransform(source, transform)
		},
		arg: func(row getl.Row, out chan getl.Row) error {
			out <- getl.Row{}
			out <- getl.Row{}
			out <- getl.Row{}
			return nil
		},
	},
}

// TestEquality tests that the chained version and non-chained version of a transform
// have the same result, given the same input and options.
func TestEquality(t *testing.T) {
	for _, config := range transforms {
		chained := tests.GetRows(config.chained(config.source(), config.arg))
		unchained := tests.GetRows(config.unchained(config.source(), config.arg))
		assert.Equal(t, chained, unchained, "%s failed", config.name)
	}
}
