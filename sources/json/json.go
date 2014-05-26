package json

import (
	"bufio"
	"encoding/json"
	"github.com/azylman/getl"
	"io"
	"os"
)

type table struct {
	err     error
	rows    chan getl.Row
	stopped bool
}

func (t table) Rows() <-chan getl.Row {
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

func (t *table) handleErr(err error) {
	if err != io.EOF {
		t.err = err
	}
}

func (t *table) start(filename string) {
	defer t.Stop()
	defer close(t.rows)

	fin, err := os.Open(filename)
	if err != nil {
		t.err = err
		return
	}
	defer fin.Close()

	scanner := bufio.NewScanner(fin)
	for scanner.Scan() {
		if t.stopped {
			break
		}
		var row getl.Row
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
func New(filename string) getl.Table {
	table := &table{
		rows: make(chan getl.Row),
	}
	go table.start(filename)
	return table
}
