package csv

import (
	"github.com/azylman/getl/tests"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"strings"
	"testing"
)

func TestCSVSource(t *testing.T) {
	table := Source("./test.csv")
	tests.HasRows(t, table, 3)
	assert.Nil(t, table.Err())
}

func TestStop(t *testing.T) {
	tests.Stop(t, Source("./test.csv"))
}

func readFile(filename string) ([]string, error) {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(content), "\n")
	return lines, nil
}

func TestCSVSink(t *testing.T) {
	source := Source("./test.csv")
	err := Sink(source, "./test_write.csv")
	assert.Nil(t, err)
	expected, err := readFile("./test_write.csv")
	assert.Nil(t, err)
	actual, err := readFile("./test.csv")
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
}
