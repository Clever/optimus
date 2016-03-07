package error

import (
	"gopkg.in/Clever/optimus.v3"
)

// Table implemements an Optimus Table
// It's purpose is to return a given error
type Table struct {
	rows    chan optimus.Row
	err     error
	Stopped bool
}

// Err returns an ErrorTable's Error
func (e *Table) Err() error {
	return e.err
}

// Rows returns the chan for an ErrorTable's Rows
// note this should only return an error
func (e *Table) Rows() <-chan optimus.Row {
	return e.rows
}

// Stop fulfills the requirement for ErrorTable
// to implement the Stop function of an Optimus Table
func (e *Table) Stop() {
	e.Stopped = true
}

// New returns a new Table that returns a given error. Primarily used for testing purposes.
func New(err error) *Table {
	table := &Table{err: err, rows: make(chan optimus.Row)}
	close(table.rows)
	return table
}
