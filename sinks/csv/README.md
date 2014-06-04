# csv
--
    import "github.com/azylman/optimus/sinks/csv"


## Usage

#### func  New

```go
func New(source optimus.Table, filename string) error
```
New writes all of the Rows in a Table to a CSV file.
