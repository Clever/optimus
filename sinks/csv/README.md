# csv
--
    import "github.com/azylman/getl/sinks/csv"


## Usage

#### func  New

```go
func New(source getl.Table, filename string) error
```
New writes all of the Rows in a Table to a CSV file.
