# transforms
--
    import "github.com/azylman/optimus/transforms"


## Usage

```go
const (
	// LeftJoin - Always add row from Left table, even if no corresponding rows found in Right table)
	LeftJoin = 1
	// InnerJoin - Only add row from Left table if corresponding row(s) found in Right table)
	InnerJoin = 2
)
```

#### func  Each

```go
func Each(fn func(optimus.Row) error) optimus.TransformFunc
```
Each returns a Table that passes through all the Rows from the source table,
invoking a function for each.

#### func  Fieldmap

```go
func Fieldmap(mappings map[string][]string) optimus.TransformFunc
```
Fieldmap returns a Table that has all the Rows of the input Table with the field
mapping applied.

#### func  Join

```go
func Join(rightTable optimus.Table, leftHeader string, rightHeader string, joinType int) optimus.TransformFunc
```
Join returns a Table that combines fields with another table, joining via
joinType

#### func  Map

```go
func Map(transform func(optimus.Row) (optimus.Row, error)) optimus.TransformFunc
```
Map returns a Table that returns the results of calling the transform function
for every row.

#### func  Select

```go
func Select(filter func(optimus.Row) (bool, error)) optimus.TransformFunc
```
Select returns a Table that only has Rows that pass the filter.

#### func  TableTransform

```go
func TableTransform(transform func(optimus.Row, chan<- optimus.Row) error) optimus.TransformFunc
```
TableTransform returns a Table that has applies the given transform function to
the output channel.

#### func  Valuemap

```go
func Valuemap(mappings map[string]map[interface{}]interface{}) optimus.TransformFunc
```
Valuemap returns a Table that has all the Rows of the input Table with a value
mapping applied.
