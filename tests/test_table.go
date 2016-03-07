package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
)

// Stop tests that a Table correctly implements Stop.
// It assumes that it is invoked with a newly-created Table.
func Stop(t *testing.T, table optimus.Table) {
	table.Stop()
	Consumed(t, table)
	assert.Nil(t, table.Err())
}

// Consumed tests that a table has been completely consumed:
// that is to say, there are no more remaining Rows to read.
func Consumed(t *testing.T, table optimus.Table) {
	HasRows(t, table, 0)
}

// HasRows tests that a table has the correct number of rows, and returns all the rows.
func HasRows(t *testing.T, table optimus.Table, expected int) []optimus.Row {
	rows := GetRows(table)
	assert.Equal(t, expected, len(rows), "expected %d rows, found %d: %#v", expected, len(rows), rows)
	return rows
}

// GetRows returns a slice of all the rows in a table
func GetRows(table optimus.Table) []optimus.Row {
	rows := []optimus.Row{}
	for row := range table.Rows() {
		rows = append(rows, row)
	}
	return rows
}

// TableCompareConfig contains a config for comparing two tables in a set of tests
type TableCompareConfig struct {
	Name     string
	Source   func() optimus.Table
	Actual   func(optimus.Table, interface{}) optimus.Table
	Expected func(optimus.Table, interface{}) optimus.Table
	Arg      interface{}
	Error    error
}

// CompareTables takes in a config of comparisons and runs them
func CompareTables(t *testing.T, configs []TableCompareConfig) {
	for _, config := range configs {
		if config.Source == nil {
			config.Source = func() optimus.Table {
				return nil
			}
		}
		actualTable := config.Actual(config.Source(), config.Arg)
		actual := GetRows(actualTable)
		if config.Expected != nil {
			expected := GetRows(config.Expected(config.Source(), config.Arg))
			for idx, expectedRow := range expected {
				for fieldName := range expectedRow {
					assert.Equal(t, expected[idx][fieldName], actual[idx][fieldName], "%s failed", config.Name)
				}
			}
		} else if config.Error != nil {
			assert.Equal(t, config.Error, actualTable.Err())
		} else {
			t.Fatalf("what are you doing?? config has neither expected nor error: %#v", config)
		}
	}
}
