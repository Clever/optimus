package transform

import (
	"github.com/azylman/getl/table/csv"
	"testing"
)

func TestFieldmap(t *testing.T) {
	table := csv.New("./test.csv")
	transformedTable := Fieldmap(table, map[string][]string{"header1": {"header4"}})
	for row := range transformedTable.Rows() {
		t.Logf("got row %#v", row)
	}
}

func TestFieldmapChain(t *testing.T) {
	table := csv.New("./test.csv")
	transformedTable := NewTransformer(table).Fieldmap(map[string][]string{"header1": {"header4"}}).Table()
	for row := range transformedTable.Rows() {
		t.Logf("got row %#v", row)
	}
}
