package transform

import (
	"errors"
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/infinite"
	"github.com/azylman/getl/sources/slice"
	"github.com/azylman/getl/tests"
	"github.com/stretchr/testify/assert"
	"testing"
)

var input = []getl.Row{
	{"header1": "value1", "header2": "value2"},
	{"header1": "value3", "header2": "value4"},
	{"header1": "value5", "header2": "value6"},
}

var expected = []getl.Row{
	{"header4": "value1"},
	{"header4": "value3"},
	{"header4": "value5"},
}

var fieldMapping = map[string][]string{"header1": {"header4"}}

// Test that field mapping behaves as expected
func TestFieldmap(t *testing.T) {
	table := slice.New(input)
	transformedTable := Fieldmap(table, fieldMapping)
	rows := tests.HasRows(t, transformedTable, 3)
	assert.Equal(t, expected, rows)
}

// Test that field mapping via a Transformer behaves as expted
func TestFieldmapChain(t *testing.T) {
	table := slice.New(input)
	transformedTable := NewTransformer(table).Fieldmap(fieldMapping).Table()
	rows := tests.HasRows(t, transformedTable, 3)
	assert.Equal(t, expected, rows)
}

// Test that chaining together multiple transforms behaves as expected
func TestChaining(t *testing.T) {
	table := slice.New(input)
	expected := []getl.Row{
		{"header1": "value1"},
		{"header1": "value3"},
		{"header1": "value5"},
	}
	transformedTable := NewTransformer(table).Fieldmap(
		fieldMapping).Fieldmap(map[string][]string{"header4": {"header1"}}).Table()
	rows := tests.HasRows(t, transformedTable, 3)
	assert.Equal(t, expected, rows)
}

// TestTransformError tests that the upstream Table had all of its data consumed in the case of an
// error.
func TestTransformError(t *testing.T) {
	in := infinite.New()
	out := elTransform(in, func(row getl.Row) (getl.Row, error) {
		return nil, errors.New("some error")
	})
	// Should receive no rows here because the first response was an error.
	tests.Consumed(t, out)
	// Should receive no rows here because the the transform should have consumed
	// all the rows.
	tests.Consumed(t, in)
}
