package json

import (
	"bytes"
	"errors"
	"testing"

	errorSource "github.com/Clever/optimus/v4/sources/error"
	"github.com/Clever/optimus/v4/sources/json"
	"github.com/stretchr/testify/assert"
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
	source := errorSource.New(errors.New("failed"))
	assert.EqualError(t, New(&bytes.Buffer{})(source), "failed")
	assert.True(t, source.Stopped)
}
