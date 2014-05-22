package transformer

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/slice"
	"testing"
)

var transformEqualities = []tableCompareConfig{
	{
		name:   "Fieldmap",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			mappings := arg.(map[string][]string)
			return Fieldmap(source, mappings)
		},
		expected: func(getl.Table, interface{}) getl.Table {
			return slice.New([]getl.Row{
				{"header4": "value1"},
				{"header4": "value3"},
				{"header4": "value5"},
			})
		},
		arg: map[string][]string{"header1": {"header4"}},
	},
}

func TestTransforms(t *testing.T) {
	compareTables(t, transformEqualities)
}
