package infinite

import (
	"github.com/azylman/getl"
)

type infiniteTable struct {
	rows    chan getl.Row
	stopped bool
}

func (i infiniteTable) Rows() <-chan getl.Row {
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
	defer i.Stop()
	for {
		if i.stopped {
			break
		}
		i.rows <- map[string]interface{}{}
	}
}

// New creates a new Table that infinitely sends empty rows.
func New() getl.Table {
	table := &infiniteTable{rows: make(chan getl.Row)}
	go table.load()
	return table
}
