package error

import (
	"gopkg.in/Clever/optimus.v2"
)

type errorTable struct {
	rows chan optimus.Row
	err  error
}

func (e errorTable) Err() error {
	return e.err
}

func (e errorTable) Rows() <-chan optimus.Row {
	return e.rows
}

func (e errorTable) Stop() {}

// New returns a new Table that returns a given error. Primarily used for testing purposes.
func New(err error) optimus.Table {
	table := &errorTable{err: err, rows: make(chan optimus.Row)}
	close(table.rows)
	return table
}
