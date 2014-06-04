package csv

import (
	"encoding/csv"
	"github.com/azylman/optimus"
	"io"
	"os"
)

type table struct {
	err     error
	rows    chan optimus.Row
	stopped bool
}

func (t *table) start(filename string) {
	defer t.Stop()
	defer close(t.rows)

	fin, err := os.Open(filename)
	defer fin.Close()

	reader := csv.NewReader(fin)

	headers, err := reader.Read()
	if err != nil {
		t.handleErr(err)
		return
	}

	reader.FieldsPerRecord = len(headers)
	for {
		if t.stopped {
			break
		}
		line, err := reader.Read()
		if err != nil {
			t.handleErr(err)
			return
		}
		t.rows <- convertLineToRow(line, headers)
	}
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

func (t *table) handleErr(err error) {
	if err != io.EOF {
		t.err = err
	}
}

func convertLineToRow(line []string, headers []string) optimus.Row {
	row := optimus.Row{}
	for i, header := range headers {
		row[header] = line[i]
	}
	return row
}

// New returns a new Table that scans over the rows of a CSV.
func New(filename string) optimus.Table {
	table := &table{
		rows: make(chan optimus.Row),
	}
	go table.start(filename)
	return table
}
