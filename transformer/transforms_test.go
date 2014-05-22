package transformer

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/slice"
	"github.com/azylman/getl/tests"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Test that field mapping behaves as expected
func TestFieldmap(t *testing.T) {
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
	fieldMapping := map[string][]string{"header1": {"header4"}}

	table := slice.New(input)
	transformedTable := Fieldmap(table, fieldMapping)
	rows := tests.HasRows(t, transformedTable, 3)
	assert.Equal(t, expected, rows)
}
