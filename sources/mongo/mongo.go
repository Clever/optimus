package mongo

import (
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/mgo.v2"
)

type table struct {
	err     error
	rows    chan optimus.Row
	stopped bool
}

func (t *table) start(q *mgo.Query) {
	defer t.Stop()
	defer close(t.rows)

	i := q.Iter()
	for !t.stopped {
		r := optimus.Row{}
		if !i.Next(&r) {
			break
		}
		t.rows <- r
	}
	t.err = i.Err()
}

func (t table) Rows() <-chan optimus.Row {
	return t.rows
}

func (t table) Err() error {
	return t.err
}

func (t *table) Stop() {
	if t.stopped {
		return
	}
	t.stopped = true
}

// New returns a new Table that iterates over all the results of a mongo query.
func New(q *mgo.Query) optimus.Table {
	t := &table{rows: make(chan optimus.Row)}
	go t.start(q)
	return t
}
