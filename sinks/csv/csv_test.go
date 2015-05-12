package csv

import (
	"bytes"
	csvEncoding "encoding/csv"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/csv"
	errorSource "gopkg.in/Clever/optimus.v3/sources/error"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
)

var csvData = `header1,header2,header3
field1,field2,field3
field4,field5,field6
field7,field8,field9
`

var tabData = "header1\theader2\theader3\nfield1\tfield2\tfield3\nfield4\tfield5\tfield6\nfield7\tfield8\tfield9\n"

func TestCSVSink(t *testing.T) {
	actual := &bytes.Buffer{}
	assert.Nil(t, New(actual)(csv.New(bytes.NewBufferString(csvData))))
	assert.Equal(t, actual.String(), csvData)
}

func TestTabSync(t *testing.T) {
	actual := &bytes.Buffer{}
	writer := csvEncoding.NewWriter(actual)
	writer.Comma = '\t'
	assert.Nil(t, NewWithCsvWriter(writer)(csv.New(bytes.NewBufferString(csvData))))
	assert.Equal(t, actual.String(), tabData)
}

func TestNilValues(t *testing.T) {
	rows := []optimus.Row{{"field1": "val1", "field2": nil}}
	actual := &bytes.Buffer{}
	assert.Nil(t, New(actual)(slice.New(rows)))
	assert.Equal(t, tests.GetRows(csv.New(actual)), rows)
}

func TestAlphabetical(t *testing.T) {
	source := slice.New([]optimus.Row{{"a": "0", "b": "0", "c": "0", "d": "0", "e": "0", "f": "0"}})
	actual := &bytes.Buffer{}
	assert.Nil(t, New(actual)(source))
	assert.Equal(t, strings.Split(actual.String(), "\n")[0], "a,b,c,d,e,f")
}

func TestCSVSinkError(t *testing.T) {
	source := errorSource.New(errors.New("failed"))
	assert.EqualError(t, New(&bytes.Buffer{})(source), "failed")
	assert.True(t, source.Stopped)
}
