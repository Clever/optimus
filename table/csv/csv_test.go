package csv

import (
	"testing"
)

func TestScansCSV(t *testing.T) {
	table := NewTable("./test.csv")
	numRows := 0
	for table.Scan() {
		numRows++
	}
	if numRows != 3 {
		t.Fatalf("expected %d rows, got %d", 3, numRows)
	}
	if table.Err() != nil {
		t.Fatal(table.Err())
	}
}
