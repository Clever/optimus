package json

import (
	"github.com/stretchr/testify/assert"
	"gopkg.in/azylman/optimus.v1"
	"gopkg.in/azylman/optimus.v1/tests"
	"testing"
)

func TestJSONSource(t *testing.T) {
	table := New("./data.json")
	expected := []optimus.Row{
		{"header1": "field1", "header2": "field2", "header3": "field3"},
		{"header1": "field4", "header2": "field5", "header3": "field6"},
		{"header1": "field7", "header2": "field8", "header3": "field9"},
	}
	assert.Equal(t, expected, tests.GetRows(table))
	assert.Nil(t, table.Err())
}

func TestStop(t *testing.T) {
	tests.Stop(t, New("./data.json"))
}
