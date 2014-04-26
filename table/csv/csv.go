package csv

import (
	"bufio"
	csvEncoding "encoding/csv"
	"github.com/azylman/getl"
	"io"
	"os"
)

type table struct {
	filename string
	offset   int64
	err      error
	row      getl.Row
	headers  []string
}

func (t *table) Scan() bool {
	// To ensure that it's impossible to leave file descriptors hanging around, we open the file,
	// read a single record, and close it again. This means that, every time, we need to seek to
	// where we last left off.
	fin, err := os.Open(t.filename)
	defer fin.Close()
	if _, err := fin.Seek(t.offset, 0); err != nil {
		t.err = err
		return false
	}
	// Internally, csv.Reader uses a bufio.Reader. This means that more data might get read than is
	// used, making our seeks inaccurate. If we pass in an instance of bufio.Reader, it won't
	// construct a new one, but will use that. This allows us to figure out how much data is
	// actually used and seek appropriately.
	buf := bufio.NewReader(fin)
	reader := csvEncoding.NewReader(buf)
	if t.headers == nil {
		headers, err := reader.Read()
		if err != nil {
			return t.handleErr(err)
		}
		t.headers = headers
	}
	reader.FieldsPerRecord = len(t.headers)
	line, err := reader.Read()
	if err != nil {
		return t.handleErr(err)
	}
	t.row = t.convertLineToRow(line)
	offset, err := fin.Seek(0, 1)
	if err != nil {
		return t.handleErr(err)
	}
	// csv.Reader actually left off at the offset of the file minus any buffered data.
	t.offset = offset - int64(buf.Buffered())
	return true
}

func (t table) Row() getl.Row {
	return t.row
}

func (t table) Err() error {
	return t.err
}

func (t *table) handleErr(err error) bool {
	if err != io.EOF {
		t.err = err
	}
	return false
}

func (t table) convertLineToRow(line []string) getl.Row {
	row := getl.Row{}
	for i, header := range t.headers {
		row[header] = line[i]
	}
	return row
}

// NewTable returns a new Table that scans over the rows of a CSV.
func NewTable(filename string) getl.Table {
	table := &table{
		filename: filename,
		offset:   0,
	}
	return table
}
