package csv

import (
	"bytes"
	"encoding/csv"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/tests"
)

var csvData = `header1,header2,header3
field1,field2,field3
field4,field5,field6
field7,field8,field9
`

var tabData = "header1\theader2\theader3\nfield1\tfield2\tfield3\nfield4\tfield5\tfield6\nfield7\tfield8\tfield9\n"

var expected = []optimus.Row{
	{"header1": "field1", "header2": "field2", "header3": "field3"},
	{"header1": "field4", "header2": "field5", "header3": "field6"},
	{"header1": "field7", "header2": "field8", "header3": "field9"}}

func TestCSVSource(t *testing.T) {
	table := New(bytes.NewBufferString(csvData))
	assert.Equal(t, expected, tests.GetRows(table))
	assert.Nil(t, table.Err())
}

func TestTabSource(t *testing.T) {
	reader := csv.NewReader(bytes.NewBufferString(tabData))
	reader.Comma = '\t'
	table := NewWithCsvReader(reader)
	assert.Equal(t, expected, tests.GetRows(table))
	assert.Nil(t, table.Err())
}

func TestStop(t *testing.T) {
	tests.Stop(t, New(bytes.NewBufferString(csvData)))
}
