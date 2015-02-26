package transforms

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
	"testing"
)

func byStringKey(key string) func(optimus.Row, optimus.Row) bool {
	return func(i, j optimus.Row) bool {
		k := i[key].(string)
		l := j[key].(string)
		return k < l
	}
}

func byIntKey(key string) func(optimus.Row, optimus.Row) bool {
	return func(i, j optimus.Row) bool {
		k := i[key].(int)
		l := j[key].(int)
		return k < l
	}
}

var sortTests = []struct {
	input, output []optimus.Row
	less          func(optimus.Row, optimus.Row) bool
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
}

func TestSort(t *testing.T) {
	for _, sortTest := range sortTests {
		input := slice.New(sortTest.input)
		sort := Sort(sortTest.less)
		table := optimus.Transform(input, sort)

		actual := tests.GetRows(table)

		assert.Equal(t, sortTest.output, actual)
		assert.Nil(t, table.Err())
	}
}
