package csv

import (
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
