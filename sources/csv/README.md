# csv
--
    import "gopkg.in/Clever/optimus.v3/sources/csv"


## Usage

#### func  New

```go
func New(in io.Reader) optimus.Table
```
New returns a new Table that scans over the rows of a CSV.

#### func  NewWithCsvReader

```go
func NewWithCsvReader(reader *csv.Reader) optimus.Table
```
NewWithCsvReader returns a new Table that scans over the rows from the csv
reader.
