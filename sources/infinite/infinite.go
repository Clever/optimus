package infinite

import (
	"gopkg.in/azylman/optimus.v2"
)

type infiniteTable struct {
	rows    chan optimus.Row
	stopped bool
}

func (i infiniteTable) Rows() <-chan optimus.Row {
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
}

func (i *infiniteTable) start() {
	defer i.Stop()
	defer close(i.rows)
	for {
		if i.stopped {
			break
		}
		i.rows <- map[string]interface{}{}
	}
}

// New creates a new Table that infinitely sends empty rows.
func New() optimus.Table {
	table := &infiniteTable{rows: make(chan optimus.Row)}
	go table.start()
	return table
}
