package slice

import (
	"gopkg.in/Clever/optimus.v3"
)

type sliceTable struct {
	rows    chan optimus.Row
	stopped bool
}

func (s sliceTable) Rows() <-chan optimus.Row {
	return s.rows
}

func (s sliceTable) Err() error {
	return nil
}

func (s *sliceTable) Stop() {
	s.stopped = true
}

func (s *sliceTable) start(slice []optimus.Row) {
	defer s.Stop()
	defer close(s.rows)
	for _, row := range slice {
		if s.stopped {
			break
		}
		s.rows <- row
	}
}

// New creates a new Table that sends all the contents of an input slice of Rows.
func New(slice []optimus.Row) optimus.Table {
	table := &sliceTable{rows: make(chan optimus.Row)}
	go table.start(slice)
	return table
}
