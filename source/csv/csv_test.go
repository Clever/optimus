package csv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCSV(t *testing.T) {
	table := New("./test.csv")
	numRows := 0
	for _ = range table.Rows() {
		numRows++
	}
	assert.Equal(t, 3, numRows)
	assert.Nil(t, table.Err())
}

func TestStop(t *testing.T) {
	table := New("./test.csv")
	table.Stop()
	numRows := 0
	for _ = range table.Rows() {
		numRows++
	}
	assert.Equal(t, 0, numRows)
	assert.Nil(t, table.Err())
}
