package csv

import (
	"errors"
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/csv"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

func readFile(filename string) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	return lines, nil
}

func TestCSVSink(t *testing.T) {
	source := csv.New("./test.csv")
	err := New(source, "./test_write.csv")
	assert.Nil(t, err)
	expected, err := readFile("./test_write.csv")
	assert.Nil(t, err)
	actual, err := readFile("./test.csv")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

type errorTable struct {
	rows chan getl.Row
}

func (e errorTable) Err() error {
	return errors.New("failed")
}

func (e errorTable) Rows() <-chan getl.Row {
	return e.rows
}

func (e errorTable) Stop() {}

func newErrorTable() getl.Table {
	table := &errorTable{rows: make(chan getl.Row)}
	close(table.rows)
	return table
}

func TestCSVSinkError(t *testing.T) {
	source := newErrorTable()
	err := New(source, "./test_write.csv")
	assert.Equal(t, err, errors.New("failed"))
}
