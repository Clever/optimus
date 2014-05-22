package transformer

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/slice"
	"github.com/azylman/getl/tests"
	"github.com/stretchr/testify/assert"
	"testing"
)

var defaultInput = func() []getl.Row {
	return []getl.Row{
		{"header1": "value1", "header2": "value2"},
		{"header1": "value3", "header2": "value4"},
		{"header1": "value5", "header2": "value6"},
	}
}

var defaultSource = func() getl.Table {
	return slice.New(defaultInput())
}

type tableCompareConfig struct {
	name     string
	source   func() getl.Table
	actual   func(getl.Table, interface{}) getl.Table
	expected func(getl.Table, interface{}) getl.Table
	arg      interface{}
}

func compareTables(t *testing.T, configs []tableCompareConfig) {
	for _, config := range configs {
		actual := tests.GetRows(config.actual(config.source(), config.arg))
		expected := tests.GetRows(config.expected(config.source(), config.arg))
		assert.Equal(t, expected, actual, "%s failed", config.name)
	}
}
