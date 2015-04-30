# transformer
--
    import "gopkg.in/Clever/optimus.v3/transformer"


## Usage

#### type Transformer

```go
type Transformer struct {
}
```

A Transformer allows you to easily chain multiple transforms on a table.

#### func  New

```go
func New(table optimus.Table) *Transformer
```
New returns a Transformer that allows you to chain transformations on a Table.

#### func (*Transformer) Apply

```go
func (t *Transformer) Apply(transform optimus.TransformFunc) *Transformer
```
Apply applies a given TransformFunc to the Transformer.

#### func (*Transformer) Concat

```go
func (t *Transformer) Concat(tables ...optimus.Table) *Transformer
```
Concat Applies a Concat transform.

#### func (*Transformer) Concurrently

```go
func (t *Transformer) Concurrently(fn optimus.TransformFunc, concurrency int) *Transformer
```
Concurrently Applies a Concurrent transform.

#### func (*Transformer) Each

```go
func (t *Transformer) Each(transform func(optimus.Row) error) *Transformer
```
Each Applies an Each transform.

#### func (*Transformer) Fieldmap

```go
func (t *Transformer) Fieldmap(mappings map[string][]string) *Transformer
```
Fieldmap Applies a Fieldmap transform.

#### func (*Transformer) Map

```go
func (t *Transformer) Map(transform func(optimus.Row) (optimus.Row, error)) *Transformer
```
Map Applies a Map transform.

#### func (*Transformer) Pair

```go
func (t *Transformer) Pair(rightTable optimus.Table, leftID, rightID transforms.RowIdentifier,
	filterFn func(optimus.Row) (bool, error)) *Transformer
```
Pair Applies a Pair transform.

#### func (*Transformer) Reduce

```go
func (t *Transformer) Reduce(fn func(optimus.Row, optimus.Row) error) *Transformer
```
Reduce Applies a Reduce transform.

#### func (*Transformer) Select

```go
func (t *Transformer) Select(filter func(optimus.Row) (bool, error)) *Transformer
```
Select Applies a Select transform.

#### func (*Transformer) Sink

```go
func (t *Transformer) Sink(sink optimus.Sink) error
```
Sink consumes all the Rows.

#### func (*Transformer) Sort

```go
func (t *Transformer) Sort(less func(i, j optimus.Row) (bool, error)) *Transformer
```
Sort Applies a Sort transform.

#### func (*Transformer) StableSort

```go
func (t *Transformer) StableSort(less func(i, j optimus.Row) (bool, error)) *Transformer
```
StableSort Applies a StableSort transform.

#### func (Transformer) Table

```go
func (t Transformer) Table() optimus.Table
```
Table returns the terminating Table in a Transformer chain.

#### func (*Transformer) TableTransform

```go
func (t *Transformer) TableTransform(transform func(optimus.Row, chan<- optimus.Row) error) *Transformer
```
TableTransform Applies a TableTransform transform.

#### func (*Transformer) Valuemap

```go
func (t *Transformer) Valuemap(mappings map[string]map[interface{}]interface{}) *Transformer
```
Valuemap Applies a Valuemap transform.
