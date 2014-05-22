package transformer

import (
	"github.com/azylman/getl"
)

// A Table that performs a given transformation on every row in the input table.
type rowTransformedTable struct {
	input     getl.Table
	transform func(getl.Row) (getl.Row, error)
	err       error
	rows      chan getl.Row
	stopped   bool
}

func (t rowTransformedTable) Rows() chan getl.Row {
	return t.rows
}

func (t rowTransformedTable) Err() error {
	return t.err
}

func (t *rowTransformedTable) Stop() {
	if t.stopped {
		return
	}
	t.stopped = true
	t.input.Stop()
	close(t.rows)
}

func (t *rowTransformedTable) load() {
	defer t.Stop()
	for input := range t.input.Rows() {
		if t.stopped {
			break
		} else if row, err := t.transform(input); err != nil {
			t.err = err
			return
		} else {
			t.rows <- row
		}
	}
	if t.input.Err() != nil {
		t.err = t.input.Err()
	}
}

// RowTransform returns a Table that applies a transform function to every row in the input table.
func RowTransform(input getl.Table, transform func(getl.Row) (getl.Row, error)) getl.Table {
	table := &rowTransformedTable{
		input:     input,
		transform: transform,
		rows:      make(chan getl.Row),
	}
	go table.load()
	return table
}
