package transform

import (
	"errors"
	"github.com/azylman/getl"
	"github.com/azylman/getl/table/csv"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFieldmap(t *testing.T) {
	table := csv.New("./test.csv")
	transformedTable := Fieldmap(table, map[string][]string{"header1": {"header4"}})
	numRows := 0
	for _ = range transformedTable.Rows() {
		numRows++
	}
	assert.Equal(t, 3, numRows)
}

func TestFieldmapChain(t *testing.T) {
	table := csv.New("./test.csv")
	transformedTable := NewTransformer(table).Fieldmap(map[string][]string{"header1": {"header4"}}).Table()
	numRows := 0
	for _ = range transformedTable.Rows() {
		numRows++
	}
	assert.Equal(t, 3, numRows)
}

type infiniteTable struct {
	rows    chan getl.Row
	stopped bool
}

func (i infiniteTable) Rows() chan getl.Row {
	return i.rows
}

func (i infiniteTable) Err() error {
	return nil
}

func (i *infiniteTable) Stop() {
	if i.stopped {
		return
	}
	i.stopped = true
	close(i.rows)
}

func (i *infiniteTable) load() {
	defer func() {
		i.Stop()
	}()
	for {
		if i.stopped {
			break
		}
		i.rows <- map[string]interface{}{}
	}
}

func newInfTable() getl.Table {
	table := &infiniteTable{rows: make(chan getl.Row)}
	go table.load()
	return table
}

// TestTransformError tests that the upstream Table had all of its data consumed in the case of an
// error.
func TestTransformError(t *testing.T) {
	in := newInfTable()
	out := elTransform(in, func(row getl.Row) (getl.Row, error) {
		return nil, errors.New("some error")
	})
	numRows := 0
	// Should receive no rows here because the first response was an error
	for _ = range out.Rows() {
		numRows++
	}
	assert.Equal(t, 0, numRows)
	// Should receive no rows here because everythng should be consumed
	for _ = range in.Rows() {
		numRows++
	}
	assert.Equal(t, 0, numRows)
}
