package csv

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/azylman/optimus.v1"
	"gopkg.in/azylman/optimus.v1/sources/csv"
	errorSource "gopkg.in/azylman/optimus.v1/sources/error"
	"gopkg.in/azylman/optimus.v1/sources/slice"
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
	assert.Nil(t, New("./data_write.csv")(csv.New("./data.csv")))
	expected, err := readFile("./data_write.csv")
	assert.Nil(t, err)
	actual, err := readFile("./data.csv")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}

func TestNilValues(t *testing.T) {
	source := slice.New([]optimus.Row{{"field1": "val1", "field2": nil}})
	assert.Nil(t, New("./data_write.csv")(source))
	actual, err := readFile("./data_write.csv")
	assert.Nil(t, err)
	assert.Equal(t, actual, []string{"field1,field2", `val1,""`, ""})
}

func TestAlphabetical(t *testing.T) {
	source := slice.New([]optimus.Row{{"a": "0", "b": "0", "c": "0", "d": "0", "e": "0", "f": "0"}})
	assert.Nil(t, New("./data_write.csv")(source))
	actual, err := readFile("./data_write.csv")
	assert.Nil(t, err)
	assert.Equal(t, actual[0], "a,b,c,d,e,f")
}

func TestCSVSinkError(t *testing.T) {
	assert.EqualError(t, New("./data_write.csv")(errorSource.New(errors.New("failed"))), "failed")
}
