package transform

import (
	"github.com/azylman/getl"
)

type transformer struct {
	table getl.Table
}

func (t transformer) Table() getl.Table {
	return t.table
}

func (t *transformer) Fieldmap(mappings map[string][]string) *transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = Fieldmap(t.table, mappings)
	return t
}

// NewTransformer returns a Transformer that allows you to chain transformations on a table.
func NewTransformer(table getl.Table) *transformer {
	return &transformer{table}
}
