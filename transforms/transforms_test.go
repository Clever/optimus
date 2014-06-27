package transforms

import (
	"errors"
	"github.com/azylman/optimus"
	"github.com/azylman/optimus/sources/infinite"
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

// Test that chaining together multiple transforms behaves as expected
func TestJoinOneToOne(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value7"},
		{"header1": "value3", "header2": "value4", "header3": "value3", "header4": "value8"},
		{"header1": "value5", "header2": "value6", "header3": "value5", "header4": "value9"},
	}

	leftTable := slice.New(defaultInput())
	rightTable := slice.New([]optimus.Row{
		{"header3": "value1", "header4": "value7"},
		{"header3": "value3", "header4": "value8"},
		{"header3": "value5", "header4": "value9"},
	})

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", InnerJoin))

	rows := tests.HasRows(t, combinedTable, 3)
	assert.Equal(t, expected, rows)
}

func TestJoinOneToNone(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value3", "header2": "value4", "header3": "value3", "header4": "value8"},
		{"header1": "value5", "header2": "value6", "header3": "value5", "header4": "value9"},
	}

	leftTable := slice.New(defaultInput())
	rightTable := slice.New([]optimus.Row{
		// 'value1' in left table maps to no rows in the right table
		{"header3": "valueNoMatch", "header4": "value7"},
		{"header3": "value3", "header4": "value8"},
		{"header3": "value5", "header4": "value9"},
	})
	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", InnerJoin))
	rows := tests.HasRows(t, combinedTable, 2)
	assert.Equal(t, expected, rows)
}

func TestJoinOneToMany(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value8"},
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value9"},
	}

	leftTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
	})
	rightTable := slice.New([]optimus.Row{
		// 'value1' in left table maps to two rows in the right table
		{"header3": "value1", "header4": "value8"},
		{"header3": "value1", "header4": "value9"},
	})
	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", InnerJoin))
	rows := tests.HasRows(t, combinedTable, 2)
	assert.Equal(t, expected, rows)
}

func TestJoinManyToOne(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1", "header4": "value8"},
		{"header1": "value1", "header2": "value3", "header3": "value1", "header4": "value8"},
	}
	leftTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value1", "header2": "value3"},
	})
	rightTable := slice.New([]optimus.Row{
		{"header3": "value1", "header4": "value8"},
	})

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", InnerJoin))

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
	leftTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value1", "header2": "value3"},
	})
	rightTable := slice.New([]optimus.Row{
		{"header3": "value1", "header4": "value4"},
		{"header3": "value1", "header4": "value5"},
	})

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", InnerJoin))

	rows := tests.HasRows(t, combinedTable, 4)
	assert.Equal(t, expected, rows)
}

func TestLeftOverwritesRight(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value3", "header2": "value2", "header3": "value1"},
	}
	leftTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
	})
	rightTable := slice.New([]optimus.Row{
		{"header3": "value1", "header1": "value3"},
	})

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", InnerJoin))

	rows := tests.HasRows(t, combinedTable, 1)
	assert.Equal(t, expected, rows)
}

// TODO: This error isn't being through yet, so the test is failing.
// func TestRightTableTransformError(t *testing.T) {
// leftTable := slice.New([]optimus.Row{
// 		{"header1": "value1", "header2": "value2"},
// 	})
// 	rightTable := slice.New([]optimus.Row{})

// 	// Returns an error immediately
// 	rightTable = optimus.Transform(rightTable, TableTransform(func(row optimus.Row, out chan<- optimus.Row) error {
// 		return errors.New("some error")
// 	}))
// 	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", InnerJoin))

// 	if combinedTable.Err() == nil {
// 		t.Fatal("Expected RightTable to report an error")
// 	}

// 	// Should receive no rows here because the first response was an error.
// 	tests.Consumed(t, table)
// 	// Should receive no rows here because the the transform should have consumed
// 	// all the rows.
// 	tests.Consumed(t, rightTable)
// }

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