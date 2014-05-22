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
	{
		name:   "RowTransform",
		source: defaultSource,
		actual: func(source getl.Table, arg interface{}) getl.Table {
			transform := arg.(func(getl.Row) (getl.Row, error))
			return RowTransform(source, transform)
		},
		expected: func(getl.Table, interface{}) getl.Table {
			rows := defaultInput()
			for _, row := range rows {
				row["troll_key"] = "troll_value"
			}
			return slice.New(rows)
		},
		arg: func(row getl.Row) (getl.Row, error) {
			row["troll_key"] = "troll_value"
			return row, nil
		},
	},
}

func TestTransforms(t *testing.T) {
	compareTables(t, transformEqualities)
}
