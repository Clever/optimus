package slice

import (
	"github.com/azylman/getl"
)

type sliceTable struct {
	rows    chan getl.Row
	stopped bool
}

func (s sliceTable) Rows() <-chan getl.Row {
	return s.rows
}

func (s sliceTable) Err() error {
	return nil
}

func (s *sliceTable) Stop() {
	if s.stopped {
		return
	}
	s.stopped = true
}

func (s *sliceTable) start(slice []getl.Row) {
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
func New(slice []getl.Row) getl.Table {
	table := &sliceTable{rows: make(chan getl.Row)}
	go table.start(slice)
	return table
}
