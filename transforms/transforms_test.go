package transforms

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sinks/discard"
	errorTable "gopkg.in/Clever/optimus.v3/sources/error"
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

func TestJoinMergePairs(t *testing.T) {
	input := []optimus.Row{
		{"left": optimus.Row{"k1": "v1", "k2": "v2"}},
		{"left": optimus.Row{"k1": "v1", "k2": "v2"}, "right": optimus.Row{"k1": "v11", "k3": "v3"}},
	}
	expected := []optimus.Row{
		{"k1": "v1", "k2": "v2"},
		{"k1": "v11", "k2": "v2", "k3": "v3"},
	}

	for i, inp := range input {
		assert.Equal(t, expected[i], mergePairs(inp))
	}
}

func TestJoinErrors(t *testing.T) {
	left := slice.New([]optimus.Row{a, b, c})
	right := errorTable.New(fmt.Errorf("garbage error"))

	table := optimus.Transform(left, Join(right, "", "", JoinType.Left))
	tests.Consumed(t, table)
	tests.Consumed(t, right)
	assert.EqualError(t, table.Err(), "garbage error")
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

func TestGroupBy(t *testing.T) {
	transform := GroupBy(KeyIdentifier("group"))
	input := []optimus.Row{
		{"group": "1", "key": "a"},
		{"group": 2, "key": "d"},
		{"group": "3", "key": "g"},
		{"group": "1", "key": 1},
		{"group": 2, "key": 2},
		{"group": "3", "key": 3},
		{"group": "1", "key": 'b'},
		{"group": 2, "key": 'e'},
		{"group": "3", "key": 'h'},
		{"group": "hello", "key": "world"},
		{"group": "hello", "val": "gopher"},
	}
	expected := []optimus.Row{
		{"id": "1", "rows": []optimus.Row{input[0], input[3], input[6]}},
		{"id": 2, "rows": []optimus.Row{input[1], input[4], input[7]}},
		{"id": "3", "rows": []optimus.Row{input[2], input[5], input[8]}},
		{"id": "hello", "rows": []optimus.Row{input[9], input[10]}},
	}

	// groupBy makes no guarantees about what order the groups are outputted in.
	// Let's manually sort them based on the group name.
	sortByGroup := func(in []optimus.Row) []optimus.Row {
		out := make([]optimus.Row, len(in), len(in))
		indexMap := map[interface{}]int{"1": 0, 2: 1, "3": 2, "hello": 3}
		for _, row := range in {
			out[indexMap[row["id"]]] = row
		}
		return out
	}

	actualTable := optimus.Transform(slice.New(input), transform)
	actual := tests.HasRows(t, actualTable, 4)
	assert.Equal(t, expected, sortByGroup(actual))
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
	for i := 0; i < 5; i++ {
		in := infinite.New()
		out := in
		for j := 0; j < i; j++ {
			out = optimus.Transform(out, Each(func(optimus.Row) error {
				return nil
			}))
		}
		out = optimus.Transform(out, TableTransform(func(row optimus.Row, out chan<- optimus.Row) error {
			return errors.New("some error")
		}))
		for j := i; j < 5; j++ {
			out = optimus.Transform(out, Each(func(optimus.Row) error {
				return nil
			}))
		}
		assert.EqualError(t, discard.Discard(out), "some error")
		tests.Consumed(t, in)
	}
}
