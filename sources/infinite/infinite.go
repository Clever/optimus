package infinite

import (
	"sync"

	"gopkg.in/Clever/optimus.v3"
)

type infiniteTable struct {
	rows    chan optimus.Row
	m       sync.Mutex
	stopped bool
}

func (i *infiniteTable) Rows() <-chan optimus.Row {
	return i.rows
}

func (i *infiniteTable) Err() error {
	return nil
}

func (i *infiniteTable) Stop() {
	i.m.Lock()
	i.stopped = true
	i.m.Unlock()
}

func (i *infiniteTable) start() {
	defer i.Stop()
	defer close(i.rows)
	for {
		i.m.Lock()
		stopped := i.stopped
		i.m.Unlock()
		if stopped {
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
