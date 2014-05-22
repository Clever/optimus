package transformer

import (
	"github.com/azylman/getl"
)

// A Transformer allows you to easily chain multiple transforms on a table.
type Transformer struct {
	table getl.Table
}

// Table returns the terminating Table in a Transformer chain.
func (t Transformer) Table() getl.Table {
	return t.table
}

// Fieldmap returns a Transformer with a field mapping transform applied.
func (t *Transformer) Fieldmap(mappings map[string][]string) *Transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = Fieldmap(t.table, mappings)
	return t
}

// New returns a Transformer that allows you to chain transformations on a table.
func New(table getl.Table) *Transformer {
	return &Transformer{table}
}
