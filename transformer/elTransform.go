package transformer

import (
	"github.com/azylman/getl"
)

// A Table that performs a given transformation on every element in the input table.
type elTransformedTable struct {
	input     getl.Table
	transform func(getl.Row) (getl.Row, error)
	err       error
	rows      chan getl.Row
	stopped   bool
}

func (t elTransformedTable) Rows() chan getl.Row {
	return t.rows
}

func (t elTransformedTable) Err() error {
	return t.err
}

func (t *elTransformedTable) Stop() {
	if t.stopped {
		return
	}
	t.stopped = true
	t.input.Stop()
	close(t.rows)
}

func (t *elTransformedTable) load() {
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

// Constructs an elTransformedTable from an input table and a transform function.
func newElTransform(input getl.Table, transform func(getl.Row) (getl.Row, error)) getl.Table {
	table := &elTransformedTable{
		input:     input,
		transform: transform,
		rows:      make(chan getl.Row),
	}
	go table.load()
	return table
}
