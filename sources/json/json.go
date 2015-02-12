package json

import (
	"encoding/json"
	"io"

	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/scanner"
)

type table struct {
	err     error
	rows    chan optimus.Row
	stopped bool
}

func (t table) Rows() <-chan optimus.Row {
	return t.rows
}

func (t table) Err() error {
	return t.err
}

func (t *table) Stop() {
	t.stopped = true
}

func (t *table) handleErr(err error) {
	if err != io.EOF {
		t.err = err
	}
}

func (t *table) start(in io.Reader) {
	defer t.Stop()
	defer close(t.rows)

	scanner := scanner.NewScanner(in)
	for scanner.Scan() {
		if t.stopped {
			break
		}
		var row optimus.Row
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.err = err
			return
		}
		t.rows <- row
	}
	if scanner.Err() != nil {
		t.err = scanner.Err()
		return
	}
}

// New returns a new Table that scans over the rows of a file of newline-separate JSON objects.
func New(in io.Reader) optimus.Table {
	table := &table{
		rows: make(chan optimus.Row),
	}
	go table.start(in)
	return table
}
