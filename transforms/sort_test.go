package transforms

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
	"testing"
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
		input := slice.New(sortTest.input)
		sort := Sort(sortTest.less)
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
