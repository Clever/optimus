# optimus
--
    import "gopkg.in/Clever/optimus.v3"

Package optimus provides interfaces and methods for lazily, concurrently
manipulating collections of data.

The Table interfaces is at the core of optimus. A Table is a lazy collection of
data. Several implementations of Tables are provided for extracting data
("sources"), such as from a CSV or Mongo.

A TransformFunc is a function that can be applied to a Table to lazily,
concurrently modify the data. Several implementations of TransformFuncs are
provided for common transformations (e.g. Map).

Lastly, a set of Sink functions are provided that will "sink" a table into some
output, such as a CSV.


### Example

Here's an example program that performs a set of field and value mappings on a
CSV file:

    package optimus

    import(
    	csvSource "gopkg.in/Clever/optimus.v3/sources/csv"
    	csvSink "gopkg.in/Clever/optimus.v3/sinks/csv"
    	"gopkg.in/Clever/optimus.v3"
    	"gopkg.in/Clever/optimus.v3/transforms"
    	"os"
    )

    func main() {
    	// Errors ignored for brevity
    	f, _ := os.Open("example1.csv")
    	out, _ := os.Create("output.csv")
    	defer out.Close()
    	begin := csvSource.New(f)
    	step1 := optimus.Transform(begin, transforms.Fieldmap(fieldMappings))
    	step2 := optimus.Transform(step1, transforms.Valuemap(valueMappings))
    	end := optimus.Transform(step2, transforms.Map(arbitraryTransformFunction))
    	csvSink.New(out)(end)
    }

Here's one that uses chaining:

    package optimus

    import(
    	csvSource "gopkg.in/Clever/optimus.v3/sources/csv"
    	csvSink "gopkg.in/Clever/optimus.v3/sinks/csv"
    	"gopkg.in/Clever/optimus.v3"
    	"gopkg.in/Clever/optimus.v3/transformer"
    	"os"
    )

    func main() {
    	// Errors ignored for brevity
    	f, _ := os.Open("example1.csv")
    	out, _ := os.Create("output.csv")
    	defer out.Close()
    	transformer.New(csvSource.New(f)).Fieldmap(fieldMappings).Valuemap(
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

## Development
You should develop Go packages from inside your Go path.
For `optimus`, that means that you should be in `$GOPATH/src/gopkg.in/Clever/optimus.v3`.
```
# Example
cd $GOPATH/src/gopkg.in/Clever
git clone git@github.com:Clever/optimus.git optimus.v3
```

## Releasing
To create a new Optimus version do the following:

0. If releasing a major version, update all references to old version
```
git grep optimus.v
```

1. Download gitsem, a tool for semantic versioning with Git
```
go get github.com/Clever/gitsem
```

2. Run gitsem in the root directory of the master Optimus branch
```
gitsem <VERSION>
```
VERSION can one of ```newversion | patch | minor | major``` as documented at (https://github.com/Clever/gitsem)

3. Push the changes to git
```
git push && git push --tags
```
