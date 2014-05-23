package transforms

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/sources/slice"
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
