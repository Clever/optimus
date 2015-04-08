# transforms
--
    import "gopkg.in/Clever/optimus.v3/transforms"

Package transforms provides a set of transformation functions that can be
applied to optimus.Tables.

For backwards-compatibility, there is a Pair transform and a Join transform.

Join is the same as Pair, except that it overwrites the fields in the left row
with the fields from the right row.

In later versions, the Join transform will be removed and Pair will be renamed
Join. The JoinType struct will also be removed in favor of the LeftJoin,
OuterJoin, etc. functions used by Pair.

## Usage

```go
var (
	// LeftJoin keeps any row where a Row was found in the left Table.
	LeftJoin = mustHave("left")
	// RightJoin keeps any row where a Row was found in the right Table.
	RightJoin = mustHave("right")
	// InnerJoin keeps any row where a Row was found in both Tables.
	InnerJoin = mustHave("left", "right")
	// OuterJoin keeps all rows.
	OuterJoin = mustHave()
)
```

```go
var JoinType = joinStruct{Left: joinType{0}, Inner: joinType{1}}
```
Left: Always add row from Left table, even if no corresponding rows found in
Right table) Inner: Only add row from Left table if corresponding row(s) found
in Right table)

#### func  Concat

```go
func Concat(tables ...optimus.Table) optimus.TransformFunc
```
Concat returns a TransformFunc that concatenates all the Rows in the input
Tables, in order.

#### func  Concurrently

```go
func Concurrently(fn optimus.TransformFunc, concurrency int) optimus.TransformFunc
```
Concurrently returns a TransformFunc that applies the given TransformFunc a
number of times concurrently, based on the supplied concurrency count.

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

#### func  GroupBy

```go
func GroupBy(identifier RowIdentifier) optimus.TransformFunc
```
GroupBy returns a TransformFunc that returns Rows of Rows grouped by their
identifier. The identifier must be comparable. Each output row is one group of
rows. The output row has two fields: id, which is the identifier for that group,
and rows, which is the slice of Rows that share that identifier. For example,
one output row in a grouping by the "group" field might look like:
optimus.Row{"id": "a", "rows": []optimus.Row{{"group": "a", "val": 2"},
{"group": "a", "val": 3}}}

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

#### func  Pair

```go
func Pair(rightTable optimus.Table, leftID, rightID RowIdentifier, filterFn func(optimus.Row) (bool, error)) optimus.TransformFunc
```
Pair returns a TransformFunc that pairs all the elements in the table with
another table, based on the given identifier functions and join type.

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

#### func  Sort

```go
func Sort(less func(i, j optimus.Row) (bool, error)) optimus.TransformFunc
```
Sort takes in a function that reports whether the row i should sort before row
j. It outputs the rows in sorted order. The sort is not guaranteed to be stable.

#### func  StableSort

```go
func StableSort(less func(i, j optimus.Row) (bool, error)) optimus.TransformFunc
```
StableSort takes in a function that reports whether the row i should sort before
row j. It outputs the rows in stably sorted order.

#### func  TableTransform

```go
func TableTransform(transform func(optimus.Row, chan<- optimus.Row) error) optimus.TransformFunc
```
TableTransform returns a TransformFunc that applies the given transform
function.

#### func  Unique

```go
func Unique(hash RowIdentifier) optimus.TransformFunc
```
Unique returns a TransformFunc that returns Rows that are unique, according to
the specified hash. No order is guaranteed for the unique row which is returned.

#### func  Valuemap

```go
func Valuemap(mappings map[string]map[interface{}]interface{}) optimus.TransformFunc
```
Valuemap returns a TransformFunc that applies a value mapping to every Row.

#### type RowIdentifier

```go
type RowIdentifier func(optimus.Row) (interface{}, error)
```

RowIdentifier takes in a row and returns something that uniquely identifies the
Row.

#### func  KeyIdentifier

```go
func KeyIdentifier(key string) RowIdentifier
```
KeyIdentifier is a convenience function that returns a RowIdentifier that
identifies the row based on the value of a key in the Row.
