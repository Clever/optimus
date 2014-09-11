package transforms

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/infinite"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
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
		Name: "Fieldmap-MapSome",
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
		Name: "Fieldmap-MapAll",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Fieldmap(map[string][]string{"header1": {"header4"}, "header2": {"header5"}}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header4": "value1", "header5": "value2"},
				{"header4": "value3", "header5": "value4"},
				{"header4": "value5", "header5": "value6"},
			})
		},
	},
	{
		Name: "Fieldmap-MapOneToMany",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Fieldmap(map[string][]string{"header1": {"header4", "header6"}}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header4": "value1", "header6": "value1"},
				{"header4": "value3", "header6": "value3"},
				{"header4": "value5", "header6": "value5"},
			})
		},
	},
	{
		Name: "Fieldmap-IgnoreInvalidMap",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Fieldmap(map[string][]string{"header1": {"header4"}, "headerFake": {"headerDoesntMap"}}))
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
	{
		Name: "Concurrently",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			mapping := map[string]map[interface{}]interface{}{
				"header1": {"value1": "value10", "value3": "value30"},
			}
			return optimus.Transform(defaultSource(), Concurrently(Valuemap(mapping), 100))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header1": "value10", "header2": "value2"},
				{"header1": "value30", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
			})
		},
	},
	{
		Name: "Reduce",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Reduce(func(accum, item optimus.Row) error {
				for key, val := range item {
					if _, ok := accum[key]; !ok {
						accum[key] = ""
					}
					accum[key] = accum[key].(string) + val.(string)
				}
				return nil
			}))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{{
				"header1": "value1value3value5",
				"header2": "value2value4value6",
			}})
		},
	},
	{
		Name: "ConcatOne",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(), Concat(defaultSource()))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
			})
		},
	},
	{
		Name: "ConcatTwoInOrder",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			newSource10 := slice.New([]optimus.Row{
				{"header1": "value10", "header2": "value20"},
				{"header1": "value30", "header2": "value40"},
				{"header1": "value50", "header2": "value60"},
			})
			newSource100 := slice.New([]optimus.Row{
				{"header1": "value100", "header2": "value200"},
				{"header1": "value300", "header2": "value400"},
				{"header1": "value500", "header2": "value600"},
			})

			return optimus.Transform(defaultSource(), Concat(newSource10, newSource100))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
				{"header1": "value10", "header2": "value20"},
				{"header1": "value30", "header2": "value40"},
				{"header1": "value50", "header2": "value60"},
				{"header1": "value100", "header2": "value200"},
				{"header1": "value300", "header2": "value400"},
				{"header1": "value500", "header2": "value600"},
			})
		},
	},
	{
		Name: "ConcatFive",
		Actual: func(optimus.Table, interface{}) optimus.Table {
			return optimus.Transform(defaultSource(),
				Concat(defaultSource(), defaultSource(), defaultSource(), defaultSource(),
					defaultSource()))
		},
		Expected: func(optimus.Table, interface{}) optimus.Table {
			return slice.New([]optimus.Row{
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
				{"header1": "value5", "header2": "value6"},
				{"header1": "value1", "header2": "value2"},
				{"header1": "value3", "header2": "value4"},
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

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Inner))

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
	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Inner))
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
	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Inner))
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

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Inner))

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

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Inner))

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

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Inner))

	rows := tests.HasRows(t, combinedTable, 1)
	assert.Equal(t, expected, rows)
}

func TestLeftJoin(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value3", "header3": "value1", "header4": "value5"},
		{"header1": "value2", "header2": "value4"},
	}
	leftTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value3"},
		{"header1": "value2", "header2": "value4"},
	})
	rightTable := slice.New([]optimus.Row{
		{"header3": "value1", "header4": "value5"},
	})

	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Left))

	rows := tests.HasRows(t, combinedTable, 2)
	assert.Equal(t, expected, rows)
}

func TestRightTableTransformError(t *testing.T) {
	leftTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
	})
	rightTable := slice.New([]optimus.Row{{"": ""}})

	// Returns an error immediately
	rightTable = optimus.Transform(rightTable, TableTransform(func(row optimus.Row, out chan<- optimus.Row) error {
		return errors.New("some error")
	}))
	combinedTable := optimus.Transform(leftTable, Join(rightTable, "header1", "header3", JoinType.Inner))

	// Should receive no rows here because the first response was an error.
	tests.Consumed(t, combinedTable)
	// Should receive no rows here because the the transform should have consumed
	// all the rows.
	tests.Consumed(t, rightTable)

	if combinedTable.Err() == nil {
		t.Fatal("Expected RightTable to report an error")
	}
}

func hashByHeader(row optimus.Row, header string) (interface{}, error) {
	val, ok := row[header]
	if !ok {
		return nil, fmt.Errorf("could not find a value for header '%s' in row %s", header, row)
	}
	return val, nil
}

func singleHeaderHash(row optimus.Row) (interface{}, error) {
	return hashByHeader(row, "header1")
}

func TestUniqueReturnsMultiple(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value2", "header2": "value4"},
		{"header1": "value3", "header2": "value6"},
	}
	inputTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value2", "header2": "value4"},
		{"header1": "value3", "header2": "value6"},
	})
	actualTable := optimus.Transform(inputTable, Unique(singleHeaderHash))

	actual := tests.HasRows(t, actualTable, 3)
	assert.Equal(t, expected, actual)
}

func TestUniqueRemovesDuplicates(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2"},
	}
	inputTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value1", "header2": "value2"},
	})
	actualTable := optimus.Transform(inputTable, Unique(singleHeaderHash))

	actual := tests.HasRows(t, actualTable, 1)
	assert.Equal(t, expected, actual)
}

func TestUniqueForSingleHeader(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2"},
	}
	inputTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value1", "header2": "value3"},
	})
	actualTable := optimus.Transform(inputTable, Unique(singleHeaderHash))

	actual := tests.HasRows(t, actualTable, 1)
	assert.Equal(t, expected, actual)
}

func invalidHeaderHash(row optimus.Row) (interface{}, error) {
	return hashByHeader(row, "invalidHeader")
}

func TestUniqueErrorForInvalidHeader(t *testing.T) {
	inputTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2"},
	})
	actualTable := optimus.Transform(inputTable, Unique(invalidHeaderHash))
	tests.Consumed(t, actualTable)
	if actualTable.Err() == nil {
		t.Fatal("Expected actualTable to report an error")
	}
}

type multiHeader struct {
	val1 interface{}
	val2 interface{}
}

func multiHeaderHash(row optimus.Row) (interface{}, error) {
	if val1, ok := row["header1"]; !ok {
		return nil, fmt.Errorf("could not find a value for header 'header1' in row %s", row)
	} else if val2, ok := row["header2"]; !ok {
		return nil, fmt.Errorf("could not find a value for header 'header2' in row %s", row)
	} else {
		// return nil, nil
		hash := multiHeader{val1: val1, val2: val2}
		return hash, nil
	}
}

// Test that chaining together multiple transforms behaves as expected
func TestUniqueForMultipleHeaders(t *testing.T) {
	expected := []optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1"},
	}
	inputTable := slice.New([]optimus.Row{
		{"header1": "value1", "header2": "value2", "header3": "value1"},
		{"header1": "value1", "header2": "value2", "header3": "value2"},
	})
	actualTable := optimus.Transform(inputTable, Unique(multiHeaderHash))

	actual := tests.HasRows(t, actualTable, 1)
	assert.Equal(t, expected, actual)
}

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
