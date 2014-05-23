package csv

import (
	csvEncoding "encoding/csv"
	"fmt"
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
	defer t.Stop()
	defer close(t.rows)

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
		t.rows <- convertLineToRow(line, headers)
	}
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

func convertLineToRow(line []string, headers []string) getl.Row {
	row := getl.Row{}
	for i, header := range headers {
		row[header] = line[i]
	}
	return row
}

// Source returns a new getl.Table that scans over the rows of a CSV.
func Source(filename string) getl.Table {
	table := &table{
		rows: make(chan getl.Row),
	}
	go table.load(filename)
	return table
}

func convertRowToRecord(row getl.Row, headers []string) []string {
	record := []string{}
	for _, header := range headers {
		record = append(record, fmt.Sprintf("%v", row[header]))
	}
	return record
}

func convertRowToHeader(row getl.Row) []string {
	header := []string{}
	for key := range row {
		header = append(header, key)
	}
	return header
}

// Sink writes all of the Rows in a Table to a CSV file.
func Sink(source getl.Table, filename string) error {
	fout, err := os.Create(filename)
	defer fout.Close()
	if err != nil {
		return err
	}
	writer := csvEncoding.NewWriter(fout)
	headers := []string{}
	wroteHeader := false
	for row := range source.Rows() {
		if !wroteHeader {
			headers = convertRowToHeader(row)
			if err := writer.Write(headers); err != nil {
				return err
			}
			wroteHeader = true
		}
		if err := writer.Write(convertRowToRecord(row, headers)); err != nil {
			return err
		}
	}
	writer.Flush()
	if writer.Error() != nil {
		return writer.Error()
	}
	return nil
}
