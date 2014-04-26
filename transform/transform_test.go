package transform

import (
	"github.com/azylman/getl/table/csv"
	"testing"
)

func TestFieldmap(t *testing.T) {
	table := csv.NewTable("./test.csv")
	transformedTable := Fieldmap(table, map[string][]string{"header1": []string{"header4"}})
	for transformedTable.Scan() {
		t.Logf("got row %#v", transformedTable.Row())
	}
}

func TestFieldmapChain(t *testing.T) {
	table := csv.NewTable("./test.csv")
	transformedTable := NewTransformer(table).Fieldmap(map[string][]string{"header1": []string{"header4"}}).Table()
	for transformedTable.Scan() {
		t.Logf("got row %#v", transformedTable.Row())
	}
}
