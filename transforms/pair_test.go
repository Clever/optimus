package transforms

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/error"
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

// First, test that we pair correctly by just using an OuterJoin so everything comes through.
func TestPairSuccess(t *testing.T) {
	for _, joinTest := range joinTests {
		left := slice.New(joinTest.left)
		right := slice.New(joinTest.right)

		pair := Pair(right, joinTest.leftHash, joinTest.rightHash, OuterJoin)
		table := optimus.Transform(left, pair)

		actual := tests.GetRows(table)

		assert.Equal(t, joinTest.expected, actual)
		assert.Nil(t, table.Err())
	}
}

// Second, test that we filter correctinly on not-outer join by running the same tests with
// different join types, except filtering the expected array.

var joinFilters = []struct {
	joinType PairType
	filter   func(optimus.Row) bool
}{
	{
		joinType: InnerJoin,
		filter: func(r optimus.Row) bool {
			return r["left"] != nil && r["right"] != nil
		},
	},
	{
		joinType: LeftJoin,
		filter: func(r optimus.Row) bool {
			return r["left"] != nil
		},
	},
	{
		joinType: RightJoin,
		filter: func(r optimus.Row) bool {
			return r["right"] != nil
		},
	},
}

func TestPairFiltering(t *testing.T) {
	filterRows := func(rows []optimus.Row, filter func(optimus.Row) bool) []optimus.Row {
		out := []optimus.Row{}
		for _, row := range rows {
			if filter(row) {
				out = append(out, row)
			}
		}
		return out
	}
	for _, joinTest := range joinTests {
		for _, joinFilter := range joinFilters {
			left := slice.New(joinTest.left)
			right := slice.New(joinTest.right)

			pair := Pair(right, joinTest.leftHash, joinTest.rightHash, joinFilter.joinType)
			table := optimus.Transform(left, pair)

			actual := tests.GetRows(table)

			assert.Equal(t, filterRows(joinTest.expected, joinFilter.filter), actual)
			assert.Nil(t, table.Err())
		}
	}
}

func TestPairErrors(t *testing.T) {
	left := slice.New([]optimus.Row{a, b, c})
	right := error.New(fmt.Errorf("garbage error"))

	table := optimus.Transform(left, Pair(right, KeyHasher(""), KeyHasher(""), OuterJoin))
	tests.Consumed(t, table)
	tests.Consumed(t, right)
	assert.EqualError(t, table.Err(), "garbage error")
}
