# csv
--
    import "gopkg.in/Clever/optimus.v3/sinks/csv"


## Usage

#### func  New

```go
func New(out io.Writer) optimus.Sink
```
New writes all of the Rows in a Table to a CSV file. It assumes that all Rows
have the same headers. Columns are written in alphabetical order.

#### func  NewWithCsvWriter

```go
func NewWithCsvWriter(writer *csv.Writer) optimus.Sink
```
NewWithCsvWriter writes all of the Rows in a Table to a CSV file using the
options in the CSV writer. It assumes that all Rows have the same headers.
Columns are written in alphabetical order.
