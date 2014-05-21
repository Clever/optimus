package csv

import (
	csvEncoding "encoding/csv"
	"github.com/azylman/getl"
	"io"
	"os"
)

type table struct {
	err     error
	rows    chan getl.Row
	stopped bool
}

func (t *table) load(filename string) {
	defer func() {
		close(t.rows)
		t.Stop()
	}()

	fin, err := os.Open(filename)
	defer fin.Close()

	reader := csvEncoding.NewReader(fin)

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
		t.rows <- t.convertLineToRow(line, headers)
	}
}

func (t table) Rows() chan getl.Row {
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

func (t table) convertLineToRow(line []string, headers []string) getl.Row {
	row := getl.Row{}
	for i, header := range headers {
		row[header] = line[i]
	}
	return row
}

// New returns a new getl.Table that scans over the rows of a CSV.
func New(filename string) getl.Table {
	table := &table{
		rows: make(chan getl.Row),
	}
	go table.load(filename)
	return table
}
