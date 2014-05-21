package csv

import (
	"testing"
)

func TestScansCSV(t *testing.T) {
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
