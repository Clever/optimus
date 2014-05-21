package csv

import (
	"github.com/azylman/getl/tests"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCSV(t *testing.T) {
	table := New("./test.csv")
	tests.HasRows(t, table, 3)
	assert.Nil(t, table.Err())
}

func TestStop(t *testing.T) {
	tests.Stop(t, New("./test.csv"))
}
