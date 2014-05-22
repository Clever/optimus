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
	source    func() getl.Table
	chained   func(getl.Table) getl.Table
	unchained func(getl.Table) getl.Table
}

var transforms = map[string]equalityConfig{
	"Fieldmap": {
		source: func() getl.Table {
			return slice.New([]getl.Row{
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
			})
		},
		chained: func(source getl.Table) getl.Table {
			return New(source).Fieldmap(map[string][]string{"header1": {"header4"}}).Table()
		},
		unchained: func(source getl.Table) getl.Table {
			return Fieldmap(source, map[string][]string{"header1": {"header4"}})
		},
	},
}

// TestEquality tests that the chained version and non-chained version of a transform
// have the same result, given the same input and options.
func TestEquality(t *testing.T) {
	for name, config := range transforms {
		chained := tests.GetRows(config.chained(config.source()))
		unchained := tests.GetRows(config.unchained(config.source()))
		assert.Equal(t, chained, unchained, "%s failed", name)
	}
}
