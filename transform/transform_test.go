package transform

import (
	"errors"
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/csv"
	"github.com/azylman/getl/sources/infinite"
	"github.com/azylman/getl/tests"
	"testing"
)

func TestFieldmap(t *testing.T) {
	table := csv.New("./test.csv")
	transformedTable := Fieldmap(table, map[string][]string{"header1": {"header4"}})
	tests.HasRows(t, transformedTable, 3)
}

func TestFieldmapChain(t *testing.T) {
	table := csv.New("./test.csv")
	transformedTable := NewTransformer(table).Fieldmap(map[string][]string{"header1": {"header4"}}).Table()
	tests.HasRows(t, transformedTable, 3)
}

// TestTransformError tests that the upstream Table had all of its data consumed in the case of an
// error.
func TestTransformError(t *testing.T) {
	in := infinite.New()
	out := elTransform(in, func(row getl.Row) (getl.Row, error) {
		return nil, errors.New("some error")
	})
	// Should receive no rows here because the first response was an error.
	tests.Consumed(t, out)
	// Should receive no rows here because the the transform should have consumed
	// all the rows.
	tests.Consumed(t, in)
}
