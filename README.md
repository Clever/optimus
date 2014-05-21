# getl
--
    import "github.com/azylman/getl"

Package getl provides methods for manipulating tables of data.


### Example

Here's an example program that performs a set of field and value mappings on a
CSV file:

    package getl

    import(
    	"github.com/azylman/getl/table/csv"
    	"github.com/azylman/getl/transform"
    )

    func main() {
    	begin := csv.NewTable("example1.csv")
    	step1 := transform.Fieldmap(begin, fieldMappings)
    	end := transform.Valuemap(step1, valueMappings)
    	err := csv.FromTable(end, "output.csv")
    }

Here's one that uses chaining:

    package getl

    import(
    	"github.com/azylman/getl/table/csv"
    	"github.com/azylman/getl/transform"
    )

    func main() {
    	begin := csv.NewTable("example1.csv")
    	end, err := transform.NewTransformer(begin)
    		.Fieldmap(fieldMappings)
    		.Valuemap(valueMappings)
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
	Rows() chan Row
	// Err returns the first non-EOF error that was encountered by the Table.
	Err() error
	// Stop signifies that a Table should stop sending Rows down its channel.
	// A Table is also responsible for calling Stop on any upstream Tables it knows about.
	// Stop should be idempotent.
	Stop()
}
```

Table is a representation of a table of data.
