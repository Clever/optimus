package transforms

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
)

func byStringKey(key string) func(optimus.Row, optimus.Row) (bool, error) {
	return func(i, j optimus.Row) (bool, error) {
		k, ok := i[key].(string)
		if !ok {
			return false, fmt.Errorf("%s wasn't a string, had value: %#v", key, i[key])
		}
		l, ok := j[key].(string)
		if !ok {
			return false, fmt.Errorf("%s wasn't a string, had value: %#v", key, j[key])
		}
		return k < l, nil
	}
}

func byIntKey(key string) func(optimus.Row, optimus.Row) (bool, error) {
	return func(i, j optimus.Row) (bool, error) {
		k, ok := i[key].(int)
		if !ok {
			return false, fmt.Errorf("%s wasn't an int, had value: %#v", key, i[key])
		}
		l, ok := j[key].(int)
		if !ok {
			return false, fmt.Errorf("%s wasn't an int, had value: %#v", key, j[key])
		}
		return k < l, nil
	}
}

var sortTests = []struct {
	input, output []optimus.Row
	less          func(optimus.Row, optimus.Row) (bool, error)
	err           error
}{
	{
		input:  []optimus.Row{{"a": "q"}, {"a": "b", "b": "d"}, {"a": "c"}},
		output: []optimus.Row{{"a": "b", "b": "d"}, {"a": "c"}, {"a": "q"}},
		less:   byStringKey("a"),
	},
	{
		input:  []optimus.Row{{"a": 4}, {"a": 2, "b": "d"}, {"a": 1}},
		output: []optimus.Row{{"a": 1}, {"a": 2, "b": "d"}, {"a": 4}},
		less:   byIntKey("a"),
	},
	{
		input:  []optimus.Row{{"a": "4"}, {"a": "4", "b": "4"}},
		output: []optimus.Row{},
		less:   byIntKey("a"),
		err:    fmt.Errorf(`a wasn't an int, had value: "4"`),
	},
}

func TestSort(t *testing.T) {
	for _, sortTest := range sortTests {
		for _, sort := range []optimus.TransformFunc{Sort(sortTest.less), StableSort(sortTest.less)} {
			input := slice.New(sortTest.input)
			table := optimus.Transform(input, sort)

			actual := tests.GetRows(table)

			if sortTest.err != nil {
				assert.Equal(t, sortTest.err, table.Err())
			} else {
				assert.Equal(t, actual, sortTest.output)
				assert.Nil(t, table.Err())
			}
		}
	}
}

func TestStable(t *testing.T) {
	input := []optimus.Row{
		{"c": "a", "b": "a"},
		{"c": "a", "b": "b"},
		{"c": "a", "b": "c"},
		{"c": "a", "b": "d"},
		{"c": "a", "b": "e"},
		{"c": "a", "b": "f"},
		{"c": "a", "b": "g"},
		{"c": "a", "b": "h"},
		{"c": "a", "b": "i"},
		{"c": "a", "b": "j"},
		{"c": "a", "b": "k"},
		{"c": "a", "b": "l"},
		{"c": "a", "b": "m"},
		{"c": "a", "b": "n"},
		{"c": "a", "b": "o"},
		{"c": "a", "b": "p"},
		{"c": "a", "b": "q"},
		{"c": "a", "b": "r"},
		{"c": "a", "b": "s"},
		{"c": "a", "b": "t"},
		{"c": "a", "b": "u"},
		{"c": "a", "b": "v"},
		{"c": "a", "b": "w"},
		{"c": "a", "b": "x"},
		{"c": "a", "b": "y"},
		{"c": "a", "b": "z"},
	}

	table := optimus.Transform(slice.New(input), StableSort(byStringKey("c")))
	actual := tests.GetRows(table)
	assert.Nil(t, table.Err())
	assert.Equal(t, actual, input)
}
