package transformer

import (
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/transforms"
)

// A Transformer allows you to easily chain multiple transforms on a table.
type Transformer struct {
	table optimus.Table
}

// Table returns the terminating Table in a Transformer chain.
func (t Transformer) Table() optimus.Table {
	return t.table
}

// Apply applies a given TransformFunc to the Transformer.
func (t *Transformer) Apply(transform optimus.TransformFunc) *Transformer {
	// TODO: Should this return a new transformer instead of modifying the existing one?
	t.table = optimus.Transform(t.table, transform)
	return t
}

// Fieldmap Applies a Fieldmap transform.
func (t *Transformer) Fieldmap(mappings map[string][]string) *Transformer {
	return t.Apply(transforms.Fieldmap(mappings))
}

// Map Applies a Map transform.
func (t *Transformer) Map(transform func(optimus.Row) (optimus.Row, error)) *Transformer {
	return t.Apply(transforms.Map(transform))
}

// Each Applies an Each transform.
func (t *Transformer) Each(transform func(optimus.Row) error) *Transformer {
	return t.Apply(transforms.Each(transform))
}

// TableTransform Applies a TableTransform transform.
func (t *Transformer) TableTransform(transform func(optimus.Row, chan<- optimus.Row) error) *Transformer {
	return t.Apply(transforms.TableTransform(transform))
}

// Select Applies a Select transform.
func (t *Transformer) Select(filter func(optimus.Row) (bool, error)) *Transformer {
	return t.Apply(transforms.Select(filter))
}

// Valuemap Applies a Valuemap transform.
func (t *Transformer) Valuemap(mappings map[string]map[interface{}]interface{}) *Transformer {
	return t.Apply(transforms.Valuemap(mappings))
}

// Reduce Applies a Reduce transform.
func (t *Transformer) Reduce(fn func(optimus.Row, optimus.Row) error) *Transformer {
	return t.Apply(transforms.Reduce(fn))
}

// Concurrently Applies a Concurrent transform.
func (t *Transformer) Concurrently(fn optimus.TransformFunc, concurrency int) *Transformer {
	return t.Apply(transforms.Concurrently(fn, concurrency))
}

// Concat Applies a Concat transform.
func (t *Transformer) Concat(tables ...optimus.Table) *Transformer {
	return t.Apply(transforms.Concat(tables...))
}

// Pair Applies a Pair transform.
func (t *Transformer) Pair(rightTable optimus.Table, leftID, rightID transforms.RowIdentifier,
	filterFn func(optimus.Row) (bool, error)) *Transformer {
	return t.Apply(transforms.Pair(rightTable, leftID, rightID, filterFn))
}

// Sort Applies a Sort transform.
func (t *Transformer) Sort(less func(i, j optimus.Row) (bool, error)) *Transformer {
	return t.Apply(transforms.Sort(less))
}

// StableSort Applies a StableSort transform.
func (t *Transformer) StableSort(less func(i, j optimus.Row) (bool, error)) *Transformer {
	return t.Apply(transforms.StableSort(less))
}

// Sink consumes all the Rows.
func (t *Transformer) Sink(sink optimus.Sink) error {
	return sink(t.table)
}

// New returns a Transformer that allows you to chain transformations on a Table.
func New(table optimus.Table) *Transformer {
	return &Transformer{table}
}
