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

// RowTransform returns a Transformer with a transform applied.
func (t *Transformer) RowTransform(transform func(getl.Row) (getl.Row, error)) *Transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = RowTransform(t.table, transform)
	return t
}

// TableTransform returns a Transformer with a transform applied.
func (t *Transformer) TableTransform(transform func(getl.Row, chan getl.Row) error) *Transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = TableTransform(t.table, transform)
	return t
}

// Select returns a Transformer with a filter applied.
func (t *Transformer) Select(filter func(getl.Row) (bool, error)) *Transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = Select(t.table, filter)
	return t
}

// New returns a Transformer that allows you to chain transformations on a table.
func New(table getl.Table) *Transformer {
	return &Transformer{table}
}
