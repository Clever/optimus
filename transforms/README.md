# transforms
--
    import "gopkg.in/Clever/optimus.v3/transforms"

Package transforms provides a set of transformation functions that can be
applied to optimus.Tables.

For backwards-compatibility, there is a Pair transform and a Join transform.

Join is the same as Pair, except that it overwrites the fields in the left row
with the fields from the right row.

In later versions, the Join transform will be removed and Pair will be renamed
Join.

Until then, there's a PairType used as input to Pair and all the values of it
are called Join. Deal with it.

## Usage

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
func Pair(rightTable optimus.Table, leftHash, rightHash RowHasher, join PairType) optimus.TransformFunc
```
Pair returns a TransformFunc that pairs all the elements in the table with
another table, based on the given hashing functions and join type.

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

#### func  Unique

```go
func Unique(hash UniqueHash) optimus.TransformFunc
```
Unique returns a TransformFunc that returns Rows that are unique, according to
the specified hash. No order is guaranteed for the unique row which is returned.

#### func  Valuemap

```go
func Valuemap(mappings map[string]map[interface{}]interface{}) optimus.TransformFunc
```
Valuemap returns a TransformFunc that applies a value mapping to every Row.

#### type PairType

```go
type PairType int
```

PairType is the type of join to use when Pairing

```go
const (
	// LeftJoin keeps any row where a Row was found in the left Table.
	LeftJoin PairType = iota
	// RightJoin keeps any row where a Row was found in the right Table.
	RightJoin
	// InnerJoin keeps any row where a Row was found in both Tables.
	InnerJoin
	// OuterJoin keeps all rows.
	OuterJoin
)
```

#### type RowHasher

```go
type RowHasher func(optimus.Row) interface{}
```

RowHasher takes in a row and returns a hash for that Row. Used when Pairing.

#### func  KeyHasher

```go
func KeyHasher(key string) RowHasher
```
KeyHasher is a convenience function that returns a RowHasher that hashes based
on the value of a key in the Row.

#### type UniqueHash

```go
type UniqueHash func(optimus.Row) (interface{}, error)
```

UniqueHash takes an optimus.Row and returns a hashed value
