package transformer

import (
	"github.com/azylman/getl"
)

type tableTransformedTable struct {
	input   getl.Table
	err     error
	rows    chan getl.Row
	stopped bool
}

func (t tableTransformedTable) Rows() chan getl.Row {
	return t.rows
}

func (t tableTransformedTable) Err() error {
	return t.err
}

func (t *tableTransformedTable) Stop() {
	if t.stopped {
		return
	}
	t.stopped = true
	t.input.Stop()
	close(t.rows)
}

func (t *tableTransformedTable) load(transform func(getl.Row, chan getl.Row) error) {
	defer t.Stop()
	for row := range t.input.Rows() {
		if t.stopped {
			break
		} else if err := transform(row, t.rows); err != nil {
			t.err = err
			return
		}
	}
	if t.input.Err() != nil {
		t.err = t.input.Err()
	}
}

// TableTransform returns a Table that has applies the given transform function to the output channel.
func TableTransform(input getl.Table, transform func(getl.Row, chan getl.Row) error) getl.Table {
	table := &tableTransformedTable{
		input: input,
		rows:  make(chan getl.Row),
	}
	go table.load(transform)
	return table
}
