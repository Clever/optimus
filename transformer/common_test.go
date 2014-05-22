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
	error    error
}

func compareTables(t *testing.T, configs []tableCompareConfig) {
	for _, config := range configs {
		if config.source == nil {
			config.source = func() getl.Table {
				return nil
			}
		}
		actualTable := config.actual(config.source(), config.arg)
		actual := tests.GetRows(actualTable)
		if config.expected != nil {
			expected := tests.GetRows(config.expected(config.source(), config.arg))
			assert.Equal(t, expected, actual, "%s failed", config.name)
		} else if config.error != nil {
			assert.Equal(t, config.error, actualTable.Err())
		} else {
			t.Fatalf("what are you doing?? config has neither expected nor error: %#v", config)
		}
	}
}
