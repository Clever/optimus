package csv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCSV(t *testing.T) {
	table := New("./test.csv")
	numRows := 0
	for row := range table.Rows() {
		t.Logf("row: %#v", row)
		numRows++
	}
	if numRows != 3 {
		t.Fatalf("expected %d rows, got %d", 3, numRows)
	}
	if table.Err() != nil {
		t.Fatal(table.Err())
	}
}

func TestStop(t *testing.T) {
	table := New("./test.csv")
	table.Stop()
	numRows := 0
	for _ = range table.Rows() {
		numRows++
	}
	assert.Equal(t, numRows, 0)
	if table.Err() != nil {
		t.Fatal(table.Err())
	}
}
