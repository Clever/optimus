package tests

import (
	"github.com/azylman/getl"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Stop tests that a Table correctly implements Stop.
// It assumes that it is invoked with a newly-created Table.
func Stop(t *testing.T, table getl.Table) {
	table.Stop()
	Consumed(t, table)
	assert.Nil(t, table.Err())
}

// Consumed tests that a table has been completely consumed:
// that is to say, there are no more remaining Rows to read.
func Consumed(t *testing.T, table getl.Table) {
	HasRows(t, table, 0)
}

// HasRows tests that a table has the correct number of rows, and returns all the rows.
func HasRows(t *testing.T, table getl.Table, expected int) []getl.Row {
	rows := GetRows(table)
	assert.Equal(t, expected, len(rows), "expected %d rows, found %d: %#v", expected, len(rows), rows)
	return rows
}

func GetRows(table getl.Table) []getl.Row {
	rows := []getl.Row{}
	for row := range table.Rows() {
		rows = append(rows, row)
	}
	return rows
}

type TableCompareConfig struct {
	Name     string
	Source   func() getl.Table
	Actual   func(getl.Table, interface{}) getl.Table
	Expected func(getl.Table, interface{}) getl.Table
	Arg      interface{}
	Error    error
}

func CompareTables(t *testing.T, configs []TableCompareConfig) {
	for _, config := range configs {
		if config.Source == nil {
			config.Source = func() getl.Table {
				return nil
			}
		}
		actualTable := config.Actual(config.Source(), config.Arg)
		actual := GetRows(actualTable)
		if config.Expected != nil {
			expected := GetRows(config.Expected(config.Source(), config.Arg))
			assert.Equal(t, expected, actual, "%s failed", config.Name)
		} else if config.Error != nil {
			assert.Equal(t, config.Error, actualTable.Err())
		} else {
			t.Fatalf("what are you doing?? config has neither expected nor error: %#v", config)
		}
	}
}
