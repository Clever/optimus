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

#### func  NewWithDelimiter

```go
func NewWithDelimiter(out io.Writer, delimiter rune) optimus.Sink
```
NewWithDelimiter writes all of the Rows in a Table to a CSV file delimited as
specified. It assumes that all Rows have the same headers. Columns are written
in alphabetical order.
