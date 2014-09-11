# csv
--
    import "gopkg.in/azylman/optimus.v2/sinks/csv"


## Usage

#### func  New

```go
func New(out io.Writer) optimus.Sink
```
New writes all of the Rows in a Table to a CSV file. It assumes that all Rows
have the same headers. Columns are written in alphabetical order.
