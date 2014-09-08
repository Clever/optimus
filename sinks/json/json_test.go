package csv

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	errorSource "gopkg.in/azylman/optimus.v1/sources/error"
	"gopkg.in/azylman/optimus.v1/sources/json"
	"testing"
)

var jsonData = `{"header1":"field1","header2":"field2","header3":"field3"}
{"header1":"field4","header2":"field5","header3":"field6"}
{"header1":"field7","header2":"field8","header3":"field9"}
`

func TestJSONSink(t *testing.T) {
	actual := &bytes.Buffer{}
	assert.Nil(t, New(actual)(json.New(bytes.NewBufferString(jsonData))))
	assert.Equal(t, actual.String(), jsonData)
}

func TestJSONSinkError(t *testing.T) {
	assert.EqualError(t, New(&bytes.Buffer{})(errorSource.New(errors.New("failed"))), "failed")
}
