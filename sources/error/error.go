package error

import (
	"gopkg.in/Clever/optimus.v3"
)

type ErrorTable struct {
	rows    chan optimus.Row
	err     error
	Stopped bool
}

func (e *ErrorTable) Err() error {
	return e.err
}

func (e *ErrorTable) Rows() <-chan optimus.Row {
	return e.rows
}

func (e *ErrorTable) Stop() {
	e.Stopped = true
}

// New returns a new Table that returns a given error. Primarily used for testing purposes.
func New(err error) *ErrorTable {
	table := &ErrorTable{err: err, rows: make(chan optimus.Row)}
	close(table.rows)
	return table
}
