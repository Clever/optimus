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

func byFloat64Key(key string) func(optimus.Row, optimus.Row) (bool, error) {
	return func(i, j optimus.Row) (bool, error) {
		k, ok := i[key].(float64)
		if !ok {
			return false, fmt.Errorf("%s wasn't an int, had value: %#v", key, i[key])
		}
		l, ok := j[key].(float64)
		if !ok {
			return false, fmt.Errorf("%s wasn't an int, had value: %#v", key, j[key])
		}
		return k < l, nil
	}
}

type sortTestCase struct {
	desc          string
	input, output []optimus.Row
	less          func(optimus.Row, optimus.Row) (bool, error)
	err           error
}

var successTests = []sortTestCase{
	{
		desc:   "letter sorting",
		input:  []optimus.Row{{"a": "q"}, {"a": "b", "b": "d"}, {"a": "c"}},
		output: []optimus.Row{{"a": "b", "b": "d"}, {"a": "c"}, {"a": "q"}},
		less:   byStringKey("a"),
	},
	{
		desc: "number sorting",
		// NOTE: we use float64 because that is what json.Unmarshal returns for number
		// types which part of the compressed sort transform.
		input:  []optimus.Row{{"a": float64(4)}, {"a": float64(2), "b": "d"}, {"a": float64(1)}},
		output: []optimus.Row{{"a": float64(1)}, {"a": float64(2), "b": "d"}, {"a": float64(4)}},
		less:   byFloat64Key("a"),
	},
}

func TestSort(t *testing.T) {
	for _, sortTest := range successTests {
		for _, sort := range []struct {
			desc   string
			sorter optimus.TransformFunc
		}{
			{desc: "regular sort", sorter: Sort(sortTest.less)},
			{desc: "stable sort", sorter: StableSort(sortTest.less)},
			{desc: "compressed stable sort", sorter: StableCompressedSort(KeyIdentifier("a"))},
		} {
			t.Run(sortTest.desc, func(t *testing.T) {
				t.Run(sort.desc, func(t *testing.T) {
					input := slice.New(sortTest.input)
					table := optimus.Transform(input, sort.sorter)

					actual := tests.GetRows(table)
					assert.NoError(t, table.Err())
					assert.Equal(t, sortTest.output, actual)
				})
			})
		}
	}
}

var errorTests = []sortTestCase{
	{
		input:  []optimus.Row{{"a": "4"}, {"a": "4", "b": "4"}},
		output: []optimus.Row{},
		less:   byFloat64Key("a"),
		err:    fmt.Errorf(`a wasn't an int, had value: "4"`),
	},
}

func TestSortError(t *testing.T) {
	for _, sortTest := range errorTests {
		for _, sort := range []optimus.TransformFunc{
			Sort(sortTest.less),
			StableSort(sortTest.less),
		} {
			input := slice.New(sortTest.input)
			table := optimus.Transform(input, sort)
			tests.GetRows(table)
			assert.Equal(t, sortTest.err, table.Err())
		}
	}
}

var stableInput = []optimus.Row{
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

func TestStable(t *testing.T) {
	table := optimus.Transform(slice.New(stableInput), StableSort(byStringKey("c")))
	actual := tests.GetRows(table)
	assert.Nil(t, table.Err())
	assert.Equal(t, actual, stableInput)
}

func TestStableCompressed(t *testing.T) {
	table := optimus.Transform(slice.New(stableInput), StableCompressedSort(KeyIdentifier("c")))
	actual := tests.GetRows(table)
	assert.Nil(t, table.Err())
	assert.Equal(t, actual, stableInput)
}
