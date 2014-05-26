# transforms
--
    import "github.com/azylman/getl/transforms"


## Usage

#### func  Each

```go
func Each(fn func(getl.Row) error) getl.TransformFunc
```
Each returns a Table that passes through all the Rows from the source table,
invoking a function for each.

#### func  Fieldmap

```go
func Fieldmap(mappings map[string][]string) getl.TransformFunc
```
Fieldmap returns a Table that has all the Rows of the input Table with the field
mapping applied.

#### func  Map

```go
func Map(transform func(getl.Row) (getl.Row, error)) getl.TransformFunc
```
Map returns a Table that returns the results of calling the transform function
for every row.

#### func  Select

```go
func Select(filter func(getl.Row) (bool, error)) getl.TransformFunc
```
Select returns a Table that only has Rows that pass the filter.

#### func  TableTransform

```go
func TableTransform(transform func(getl.Row, chan<- getl.Row) error) getl.TransformFunc
```
TableTransform returns a Table that has applies the given transform function to
the output channel.

#### func  Valuemap

```go
func Valuemap(mappings map[string]map[interface{}]interface{}) getl.TransformFunc
```
Valuemap returns a Table that has all the Rows of the input Table with a value
mapping applied.
