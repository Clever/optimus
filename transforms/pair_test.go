package transforms

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	errorSource "gopkg.in/Clever/optimus.v3/sources/error"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
	"testing"
)

var a = optimus.Row{"k1": "v1", "k3": "v3"}
var b = optimus.Row{"k2": "v3", "k4": "v4"}
var c = optimus.Row{"k5": "v5"}

var joinTests = []struct {
	left, right, expected []optimus.Row
	leftHash, rightHash   RowHasher
}{
	// All of these tests use "k1" as the key, where none of the rows should match OTHER rows, only
	// each other
	{
		left:      []optimus.Row{},
		right:     []optimus.Row{},
		expected:  []optimus.Row{},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	{
		left:  []optimus.Row{a},
		right: []optimus.Row{a},
		expected: []optimus.Row{
			{"left": a, "right": a},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	{
		left:  []optimus.Row{a},
		right: []optimus.Row{b},
		expected: []optimus.Row{
			{"left": a},
			{"right": b},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	// Trick test where they hash to nil on both sides
	{
		left:  []optimus.Row{c, b},
		right: []optimus.Row{b, c},
		expected: []optimus.Row{
			{"left": c},
			{"left": b},
			{"right": b},
			{"right": c},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	{
		left:  []optimus.Row{a, b},
		right: []optimus.Row{a},
		expected: []optimus.Row{
			{"left": a, "right": a},
			{"left": b},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	{
		left:  []optimus.Row{a},
		right: []optimus.Row{a, b},
		expected: []optimus.Row{
			{"left": a, "right": a},
			{"right": b},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	{
		left:  []optimus.Row{a, a},
		right: []optimus.Row{a, b},
		expected: []optimus.Row{
			{"left": a, "right": a},
			{"left": a, "right": a},
			{"right": b},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	{
		left:  []optimus.Row{a, b},
		right: []optimus.Row{a, a},
		expected: []optimus.Row{
			{"left": a, "right": a},
			{"left": a, "right": a},
			{"left": b},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	{
		left:  []optimus.Row{a, a},
		right: []optimus.Row{a, a},
		expected: []optimus.Row{
			{"left": a, "right": a},
			{"left": a, "right": a},
			{"left": a, "right": a},
			{"left": a, "right": a},
		},
		leftHash:  KeyHasher("k1"),
		rightHash: KeyHasher("k1"),
	},
	// Now let's get fancy and have one test with different hashes
	{
		left:  []optimus.Row{a},
		right: []optimus.Row{b},
		expected: []optimus.Row{
			{"left": a, "right": b},
		},
		leftHash:  KeyHasher("k3"),
		rightHash: KeyHasher("k2"),
	},
}

var joinFilters = []func(optimus.Row) (bool, error){LeftJoin, RightJoin, InnerJoin, OuterJoin}

func TestPairSuccess(t *testing.T) {
	filterRows := func(rows []optimus.Row, filterFn func(optimus.Row) (bool, error)) []optimus.Row {
		out := []optimus.Row{}
		for _, row := range rows {
			if f, _ := filterFn(row); f {
				out = append(out, row)
			}
		}
		return out
	}
	for _, joinTest := range joinTests {
		for _, joinFilter := range joinFilters {
			left := slice.New(joinTest.left)
			right := slice.New(joinTest.right)

			pair := Pair(right, joinTest.leftHash, joinTest.rightHash, joinFilter)
			table := optimus.Transform(left, pair)

			actual := tests.GetRows(table)

			assert.Equal(t, filterRows(joinTest.expected, joinFilter), actual)
			assert.Nil(t, table.Err())
		}
	}
}

func TestPairErrorsRightTable(t *testing.T) {
	left := slice.New([]optimus.Row{a, b, c})
	right := errorSource.New(fmt.Errorf("garbage error"))

	table := optimus.Transform(left, Pair(right, KeyHasher(""), KeyHasher(""), OuterJoin))
	tests.Consumed(t, table)
	tests.Consumed(t, right)
	assert.EqualError(t, table.Err(), "garbage error")
}

func errHasher(err error) RowHasher {
	return func(row optimus.Row) (interface{}, error) {
		return nil, err
	}
}

var joinHasherErrors = []struct {
	left, right         []optimus.Row
	leftHash, rightHash RowHasher
	expected            string
}{
	{
		left:      []optimus.Row{a},
		right:     []optimus.Row{a},
		expected:  "left error",
		leftHash:  errHasher(fmt.Errorf("left error")),
		rightHash: KeyHasher("k1"),
	},
	{
		left:      []optimus.Row{a},
		right:     []optimus.Row{a},
		expected:  "left error",
		leftHash:  KeyHasher("k1"),
		rightHash: errHasher(fmt.Errorf("left error")),
	},
}

func TestPairErrorsRowHasher(t *testing.T) {
	for _, joinHasherError := range joinHasherErrors {
		left := slice.New(joinHasherError.left)
		right := slice.New(joinHasherError.right)

		table := optimus.Transform(left, Pair(right, joinHasherError.leftHash, joinHasherError.rightHash, OuterJoin))
		tests.Consumed(t, table)
		tests.Consumed(t, right)
		assert.EqualError(t, table.Err(), joinHasherError.expected)
	}
}
