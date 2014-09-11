package csv

import (
	"errors"
	"github.com/stretchr/testify/assert"
	errorSource "gopkg.in/azylman/optimus.v1/sources/error"
	"gopkg.in/azylman/optimus.v1/sources/json"
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
	assert.Nil(t, New("./data_write.json")(json.New("./data.json")))
	expected, err := readFile("./data_write.json")
	assert.Nil(t, err)
	actual, err := readFile("./data.json")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestJSONSinkError(t *testing.T) {
	assert.EqualError(t, New("./data_write.json")(errorSource.New(errors.New("failed"))), "failed")
}
