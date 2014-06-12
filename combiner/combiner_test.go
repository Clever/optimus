package combiner

import (
	"github.com/azylman/optimus"
	"github.com/azylman/optimus/sources/slice"
	"github.com/azylman/optimus/tests"
	"github.com/stretchr/testify/assert"
	"testing"
)

var defaultInput = func() []optimus.Row {
	return []optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
	}
}

// Test that chaining together multiple transforms behaves as expected
func TestJoinOneToOne(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value7"},
		{"header1": "value3", "header2": "value4", "header3": "value3", "header4": "value8"},
		{"header1": "value5", "header2": "value6", "header3": "value5", "header4": "value9"},
	}

	table := slice.New(defaultInput())
	table2 := slice.New([]optimus.Row{
		{"header3": "value1", "header4": "value7"},
		{"header3": "value3", "header4": "value8"},
		{"header3": "value5", "header4": "value9"},
	})
	combiner := New(table, table2)
	combinedTable := combiner.Join("header1", "header3")
	rows := tests.HasRows(t, combinedTable, 3)
	assert.Equal(t, expected, rows)
}

func TestJoinOneToNone(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value3", "header2": "value4", "header3": "value3", "header4": "value8"},
		{"header1": "value5", "header2": "value6", "header3": "value5", "header4": "value9"},
	}

	table := slice.New(defaultInput())
	table2 := slice.New([]optimus.Row{
		// 'value1' in left table maps to no rows in the right table
		{"header3": "valueNoMatch", "header4": "value7"},
		{"header3": "value3", "header4": "value8"},
		{"header3": "value5", "header4": "value9"},
	})
	combiner := New(table, table2)
	combinedTable := combiner.Join("header1", "header3")
	rows := tests.HasRows(t, combinedTable, 2)
	assert.Equal(t, expected, rows)
}

func TestJoinOneToMany(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value8"},
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value9"},
	}

	table := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
	})
	table2 := slice.New([]optimus.Row{
		// 'value1' in left table maps to two rows in the right table
		{"header3": "value1", "header4": "value8"},
		{"header3": "value1", "header4": "value9"},
	})
	combiner := New(table, table2)
	combinedTable := combiner.Join("header1", "header3")
	rows := tests.HasRows(t, combinedTable, 2)
	assert.Equal(t, expected, rows)
}

func TestJoinManyToOne(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value8"},
		{"header1": "value1", "header2": "value3", "header3": "value1", "header4": "value8"},
	}
	table := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value1", "header2": "value3"},
	})
	table2 := slice.New([]optimus.Row{
		{"header3": "value1", "header4": "value8"},
	})

	combiner := New(table, table2)
	combinedTable := combiner.Join("header1", "header3")

	rows := tests.HasRows(t, combinedTable, 2)
	assert.Equal(t, expected, rows)
}

func TestJoinManyToMany(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value4"},
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value5"},
		{"header1": "value1", "header2": "value3", "header3": "value1", "header4": "value4"},
		{"header1": "value1", "header2": "value3", "header3": "value1", "header4": "value5"},
	}
	table := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value1", "header2": "value3"},
	})
	table2 := slice.New([]optimus.Row{
		{"header3": "value1", "header4": "value4"},
		{"header3": "value1", "header4": "value5"},
	})

	combiner := New(table, table2)
	combinedTable := combiner.Join("header1", "header3")

	rows := tests.HasRows(t, combinedTable, 4)
	assert.Equal(t, expected, rows)
}

func TestLeftOverwritesRight(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value3", "header2": "value2", "header3": "value1"},
	}
	table := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
	})
	table2 := slice.New([]optimus.Row{
		{"header3": "value1", "header1": "value3"},
	})

	combiner := New(table, table2)
	combinedTable := combiner.Join("header1", "header3")

	rows := tests.HasRows(t, combinedTable, 1)
	assert.Equal(t, expected, rows)
}

func TestExtend(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
		{"header1": "value7", "header2": "value8"},
	}
	table := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
	})
	table2 := slice.New([]optimus.Row{
		{"header1": "value5", "header2": "value6"},
		{"header1": "value7", "header2": "value8"},
	})

	combiner := New(table, table2)
	combinedTable := combiner.Extend()

	rows := tests.HasRows(t, combinedTable, 4)
	assert.Equal(t, expected, rows)
}
