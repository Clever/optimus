package csv

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/azylman/optimus.v1"
	"gopkg.in/azylman/optimus.v1/sources/csv"
	errorSource "gopkg.in/azylman/optimus.v1/sources/error"
	"gopkg.in/azylman/optimus.v1/sources/slice"
	"strings"
	"testing"
)

var csvData = `header1,header2,header3
field1,field2,field3
field4,field5,field6
field7,field8,field9
`

func TestCSVSink(t *testing.T) {
	actual := &bytes.Buffer{}
	assert.Nil(t, New(actual)(csv.New(bytes.NewBufferString(csvData))))
	assert.Equal(t, actual.String(), csvData)
}

func TestNilValues(t *testing.T) {
	source := slice.New([]optimus.Row{{"field1": "val1", "field2": nil}})
	actual := &bytes.Buffer{}
	assert.Nil(t, New(actual)(source))
	assert.Equal(t, actual.String(), "field1,field2\nval1,\"\"\n")
}

func TestAlphabetical(t *testing.T) {
	source := slice.New([]optimus.Row{{"a": "0", "b": "0", "c": "0", "d": "0", "e": "0", "f": "0"}})
	actual := &bytes.Buffer{}
	assert.Nil(t, New(actual)(source))
	assert.Equal(t, strings.Split(actual.String(), "\n")[0], "a,b,c,d,e,f")
}

func TestCSVSinkError(t *testing.T) {
	assert.EqualError(t, New(&bytes.Buffer{})(errorSource.New(errors.New("failed"))), "failed")
}
