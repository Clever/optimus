# transforms
--
    import "github.com/azylman/optimus/transforms"


## Usage

```go
var JoinType = joinStruct{Left: joinType{0}, Inner: joinType{1}}
```
Left: Always add row from Left table, even if no corresponding rows found in
Right table) Inner: Only add row from Left table if corresponding row(s) found
in Right table)

#### func  Concurrently

```go
func Concurrently(fn optimus.TransformFunc, concurrency int) optimus.TransformFunc
```
Concurrently returns a TransformFunc that applies the given TransformFunc with
some level of concurrency.

#### func  Each

```go
func Each(fn func(optimus.Row) error) optimus.TransformFunc
```
Each returns a TransformFunc that makes no changes to the table, but calls the
given function on every Row.

#### func  Fieldmap

```go
func Fieldmap(mappings map[string][]string) optimus.TransformFunc
```
Fieldmap returns a TransformFunc that applies a field mapping to every Row.

#### func  Join

```go
func Join(rightTable optimus.Table, leftHeader string, rightHeader string, join joinType) optimus.TransformFunc
```
Join returns a TransformFunc that joins Rows with another table using the
specified join type.

#### func  Map

```go
func Map(transform func(optimus.Row) (optimus.Row, error)) optimus.TransformFunc
```
Map returns a TransformFunc that transforms every row with the given function.

#### func  Reduce

```go
func Reduce(fn func(accum, item optimus.Row) error) optimus.TransformFunc
```
Reduce returns a TransformFunc that reduces all the Rows to a single Row.

#### func  Select

```go
func Select(filter func(optimus.Row) (bool, error)) optimus.TransformFunc
```
Select returns a TransformFunc that removes any rows that don't pass the filter.

#### func  TableTransform

```go
func TableTransform(transform func(optimus.Row, chan<- optimus.Row) error) optimus.TransformFunc
```
TableTransform returns a TransformFunc that applies the given transform
function.

#### func  Valuemap

```go
func Valuemap(mappings map[string]map[interface{}]interface{}) optimus.TransformFunc
```
Valuemap returns a TransformFunc that applies a value mapping to every Row.
