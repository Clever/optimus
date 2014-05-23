package transformer

import (
	"github.com/azylman/getl"
	"github.com/azylman/getl/transforms"
)

// A Transformer allows you to easily chain multiple transforms on a table.
type Transformer struct {
	table getl.Table
}

// Table returns the terminating Table in a Transformer chain.
func (t Transformer) Table() getl.Table {
	return t.table
}

// Apply applies a given TransformFunc to the Transformer.
func (t *Transformer) Apply(transform getl.TransformFunc) *Transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = getl.Transform(t.table, transform)
	return t
}

// Fieldmap Applies a Fieldmap transform.
func (t *Transformer) Fieldmap(mappings map[string][]string) *Transformer {
	return t.Apply(transforms.Fieldmap(mappings))
}

// RowTransform Applies a RowTransform transform.
func (t *Transformer) RowTransform(transform func(getl.Row) (getl.Row, error)) *Transformer {
	return t.Apply(transforms.RowTransform(transform))
}

// TableTransform Applies a TableTransform transform.
func (t *Transformer) TableTransform(transform func(getl.Row, chan<- getl.Row) error) *Transformer {
	return t.Apply(transforms.TableTransform(transform))
}

// Select Applies a Select transform.
func (t *Transformer) Select(filter func(getl.Row) (bool, error)) *Transformer {
	return t.Apply(transforms.Select(filter))
}

// Valuemap Applies a Valuemap transform.
func (t *Transformer) Valuemap(mappings map[string]map[interface{}]interface{}) *Transformer {
	return t.Apply(transforms.Valuemap(mappings))
}

// New returns a Transformer that allows you to chain transformations on a Table.
func New(table getl.Table) *Transformer {
	return &Transformer{table}
}
