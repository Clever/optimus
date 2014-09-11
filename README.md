# optimus
--
    import "gopkg.in/Clever/optimus.v2"

Package optimus provides methods for manipulating tables of data.


### Example

Here's an example program that performs a set of field and value mappings on a
CSV file:

    package optimus

    import(
    	csvSource "gopkg.in/Clever/optimus.v2/sources/csv"
    	csvSink "gopkg.in/Clever/optimus.v2/sinks/csv"
    	"gopkg.in/Clever/optimus.v2"
    	"gopkg.in/Clever/optimus.v2/transforms"
    	"os"
    )

    func main() {
    	f, err := os.Open("example1.csv")
    	out, err := os.Create("output.csv")
    	defer out.Close()
    	begin := csvSource.New(f)
    	step1 := optimus.Transform(begin, transforms.Fieldmap(fieldMappings))
    	step2 := optimus.Transform(step1, transforms.Valuemap(valueMappings))
    	end := optimus.Transform(step2, transforms.Map(arbitraryTransformFunction))
    	err := csvSink.New(out)(end)
    }

Here's one that uses chaining:

    package optimus

    import(
    	csvSource "gopkg.in/Clever/optimus.v2/sources/csv"
    	csvSink "gopkg.in/Clever/optimus.v2/sinks/csv"
    	"gopkg.in/Clever/optimus.v2"
    	"gopkg.in/Clever/optimus.v2/transformer"
    	"os"
    )

    func main() {
    	f, err := os.Open("example1.csv")
    	out, err := os.Create("output.csv")
    	defer out.Close()
    	begin := csvSource.New(f)
    	err := transformer.New(begin).Fieldmap(fieldMappings).Valuemap(
    		valueMappings).Map(arbitraryTransformFunction).Sink(csvSink.New(out))
    }

## Usage

#### type Row

```go
type Row map[string]interface{}
```

Row is a representation of a line of data in a Table.

#### type Sink

```go
type Sink func(Table) error
```

A Sink function takes a Table and consumes all of its Rows.

#### type Table

```go
type Table interface {
	// Rows returns a channel that provides the Rows in the table.
	Rows() <-chan Row
	// Err returns the first non-EOF error that was encountered by the Table.
	Err() error
	// Stop signifies that a Table should stop sending Rows down its channel.
	// A Table is also responsible for calling Stop on any upstream Tables it knows about.
	// Stop should be idempotent. It's expected that Stop will never be called by a consumer of a
	// Table unless that consumer is also a Table. It can be used to Stop all upstream Tables in
	// the event of an error that needs to halt the pipeline.
	Stop()
}
```

Table is a representation of a table of data.

#### func  Transform

```go
func Transform(source Table, transform TransformFunc) Table
```
Transform returns a new Table that provides all the Rows of the input Table
transformed with the TransformFunc.

#### type TransformFunc

```go
type TransformFunc func(in <-chan Row, out chan<- Row) error
```

TransformFunc is a function that can be applied to a Table to transform it. It
should receive the Rows from in and may send any number of Rows to out. It
should not return until it has finished all work (received all the Rows it's
going to receive, sent all the Rows it's going to send).
