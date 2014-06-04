package csv

import (
	"errors"
	"github.com/azylman/optimus"
	"github.com/azylman/optimus/sources/json"
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

func TestJSONSink(t *testing.T) {
	source := json.New("./data.json")
	err := New(source, "./data_write.json")
	assert.Nil(t, err)
	expected, err := readFile("./data_write.json")
	assert.Nil(t, err)
	actual, err := readFile("./data.json")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

type errorTable struct {
	rows chan optimus.Row
}

func (e errorTable) Err() error {
	return errors.New("failed")
}

func (e errorTable) Rows() <-chan optimus.Row {
	return e.rows
}

func (e errorTable) Stop() {}

func newErrorTable() optimus.Table {
	table := &errorTable{rows: make(chan optimus.Row)}
	close(table.rows)
	return table
}

func TestJSONSinkError(t *testing.T) {
	source := newErrorTable()
	err := New(source, "./data_write.json")
	assert.Equal(t, err, errors.New("failed"))
}
