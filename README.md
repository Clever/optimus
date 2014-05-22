# getl
--
    import "github.com/azylman/getl"

Package getl provides methods for manipulating tables of data.


### Example

Here's an example program that performs a set of field and value mappings on a
CSV file:

    package getl

    import(
    	"github.com/azylman/getl/sources/csv"
    	"github.com/azylman/getl/transformer"
    )

    func main() {
    	begin := csv.New("example1.csv")
    	step1 := transformer.Fieldmap(begin, fieldMappings)
    	step2 := transformer.Valuemap(step1, valueMappings)
    	end := transformer.RowTransform(step2, arbitraryTransformFunction)
    	err := csv.FromTable(end, "output.csv")
    }

Here's one that uses chaining:

    package getl

    import(
    	"github.com/azylman/getl/sources/csv"
    	"github.com/azylman/getl/transformer"
    )

    func main() {
    	begin := csv.New("example1.csv")
    	end := transformer.New(begin)
    		.Fieldmap(fieldMappings)
    		.Valuemap(valueMappings)
    		.RowTransform(arbitraryTransformFunction)
    		.Table()
    	err := csv.FromTable(end, "output.csv")
    }

## Usage

#### type Row

```go
type Row map[string]interface{}
```

Row is a representation of a line of data in a Table.

#### type Table

```go
type Table interface {
	// Rows returns a channel that provides the rows.
	Rows() <-chan Row
	// Err returns the first non-EOF error that was encountered by the Table.
	Err() error
	// Stop signifies that a Table should stop sending Rows down its channel.
	// A Table is also responsible for calling Stop on any upstream Tables it knows about.
	// Stop should be idempotent. It's expected that Stop will never be called by a consumer of a
	// Table, unless that consumer is also a Table. It can be used to Stop all upstream Tables in
	// the event of an error that needs to halt the pipeline.
	Stop()
}
```

Table is a representation of a table of data.
