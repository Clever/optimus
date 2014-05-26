package csv

import (
	"github.com/azylman/getl/tests"
	"github.com/stretchr/testify/assert"
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
